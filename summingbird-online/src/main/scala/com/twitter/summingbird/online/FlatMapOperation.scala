/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.online

import com.twitter.util.{ Future, Await }
import java.io.{ Closeable, Serializable }

// Represents the logic in the flatMap bolts
trait FlatMapOperation[-T, +U] extends Serializable with Closeable {
  def apply(t: T): Future[TraversableOnce[U]]

  override def close(): Unit = {}

  // Helper to add a simple U => S operation at the end of a FlatMapOperation
  def map[S](fn: U => S): FlatMapOperation[T, S] = new MappedOperation(this, fn)

  // Helper to add a simple U => TraversableOnce[S] operation at the end of a FlatMapOperation
  def flatMap[S](fn: U => TraversableOnce[S]): FlatMapOperation[T, S] = new FlatMappedOperation(this, fn)

  def lift[S]: FlatMapOperation[(S, T), (S, U)] = {
    val self = this

    new FlatMapOperation[(S, T), (S, U)] {
      override def apply(t: (S, T)): Future[TraversableOnce[(S, U)]] =
        self.apply(t._2).map(_.map((t._1, _)))
      override def close(): Unit = self.close()
    }
  }

  /**
   * TODO: Think about getting an implicit FutureCollector here, in
   * case we don't want to completely choke on large expansions (and
   * joins).
   */
  def andThen[V](fmo: FlatMapOperation[U, V]): FlatMapOperation[T, V] = {
    val self = this // Using the standard "self" at the top of the
    // trait caused a nullpointerexception after
    // serialization. I think that Kryo mis-serializes that reference.
    new FlatMapOperation[T, V] {
      def apply(t: T) = self(t).flatMap { tr =>
        val next: Seq[Future[TraversableOnce[V]]] = tr.map { fmo.apply(_) }.toIndexedSeq
        Future.collect(next).map(_.flatten) // flatten the inner
      }

      override def close { self.close; fmo.close }
    }
  }
}

class MappedOperation[T, Intermediate, U](
  source: FlatMapOperation[T, Intermediate],
  @transient fn: Intermediate => U
) extends FlatMapOperation[T, U] {
  val boxed = Externalizer(fn)
  lazy val built: T => Future[TraversableOnce[U]] = {
    val unboxed = boxed.get
    source match {
      case _: IdentityFlatMapOperation[_] =>
        val unboxedCasted = unboxed.asInstanceOf[T => U]
        t => Future.value(Some(unboxedCasted(t)))
      case _ => t => source.apply(t).map(_.map(unboxed))
    }
  }

  override def map[S](second: (U) => S): FlatMapOperation[T, S] =
    new MappedOperation(source, second.compose(fn))

  override def flatMap[S](second: (U) => TraversableOnce[S]): FlatMapOperation[T, S] =
    new FlatMappedOperation(source, second.compose(fn))


  override def lift[S]: FlatMapOperation[(S, T), (S, U)] =
    new MappedOperation[(S, T), (S, Intermediate), (S, U)](source.lift, si => (si._1, fn(si._2)))

  override def apply(t: T): Future[TraversableOnce[U]] = built(t)
  override def close(): Unit = source.close()
}

class FlatMappedOperation[T, Intermediate, U](
  source: FlatMapOperation[T, Intermediate],
  @transient fn: Intermediate => TraversableOnce[U]
) extends FlatMapOperation[T, U] {
  val boxed = Externalizer(fn)
  lazy val built: T => Future[TraversableOnce[U]] = {
    val unboxed = boxed.get
    source match {
      case _: IdentityFlatMapOperation[_] =>
        val unboxedCasted = unboxed.asInstanceOf[T => TraversableOnce[U]]
        t => Future.value(unboxedCasted(t))
      case _ => t => source.apply(t).map(_.flatMap(unboxed))
    }
  }

  override def map[S](second: (U) => S): FlatMapOperation[T, S] =
    new FlatMappedOperation[T, Intermediate, S](source, t => fn(t).map(second))

  override def flatMap[S](second: (U) => TraversableOnce[S]): FlatMapOperation[T, S] =
    new FlatMappedOperation[T, Intermediate, S](source, t => fn(t).flatMap(second))

  override def lift[S]: FlatMapOperation[(S, T), (S, U)] =
    new FlatMappedOperation[(S, T), (S, Intermediate), (S, U)](source.lift, si => fn(si).map((si._1, _)))

  override def apply(t: T): Future[TraversableOnce[U]] = built(t)
  override def close(): Unit = source.close()
}

class GenericFlatMapOperation[T, U](@transient fm: T => Future[TraversableOnce[U]])
    extends FlatMapOperation[T, U] {
  val boxed = Externalizer(fm)
  def apply(t: T) = boxed.get(t)
}

class IdentityFlatMapOperation[T] extends FlatMapOperation[T, T] {
  // By default we do the identity function
  def apply(t: T): Future[TraversableOnce[T]] = Future.value(Some(t))

  // But if we are composed with something else, just become it
  override def andThen[V](fmo: FlatMapOperation[T, V]): FlatMapOperation[T, V] = fmo

  override def lift[S]: FlatMapOperation[(S, T), (S, T)] = ???
}

object FlatMapOperation {
  def identity[T]: FlatMapOperation[T, T] = new IdentityFlatMapOperation()

  def apply[T, U](fm: T => TraversableOnce[U]): FlatMapOperation[T, U] =
    new IdentityFlatMapOperation[T].flatMap(fm)

  def generic[T, U](fm: T => Future[TraversableOnce[U]]): FlatMapOperation[T, U] =
    new GenericFlatMapOperation(fm)

  def combine[T, K, V, JoinedV](fmSupplier: => FlatMapOperation[T, (K, V)],
    storeSupplier: OnlineServiceFactory[K, JoinedV]): FlatMapOperation[T, (K, (V, Option[JoinedV]))] =
    new FlatMapOperation[T, (K, (V, Option[JoinedV]))] {
      lazy val fm = fmSupplier
      lazy val store = storeSupplier.serviceStore()
      override def apply(t: T) =
        fm.apply(t).flatMap { trav: TraversableOnce[(K, V)] =>
          val resultList = trav.toSeq // Can't go through this twice
          val keySet: Set[K] = resultList.map { _._1 }.toSet

          if (keySet.isEmpty)
            Future.value(Map.empty)
          else {
            // Do the lookup
            val mres: Map[K, Future[Option[JoinedV]]] = store.multiGet(keySet)
            val resultFutures = resultList.map { case (k, v) => mres(k).map { j => (k, (v, j)) } }.toIndexedSeq
            Future.collect(resultFutures)
          }
        }

      override def close {
        fm.close
        Await.result(store.close)
      }
    }

  def write[T](sinkSupplier: () => (T => Future[Unit])) =
    new WriteOperation[T](sinkSupplier)
}

class WriteOperation[T](sinkSupplier: () => (T => Future[Unit])) extends FlatMapOperation[T, T] {
  lazy val sink = sinkSupplier()
  override def apply(t: T) = sink(t).map { _ => Some(t) }
}
