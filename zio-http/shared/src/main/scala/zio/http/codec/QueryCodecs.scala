/*
 * Copyright 2021 - 2023 Sporta Technologies PVT LTD & the ZIO HTTP contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.http.codec
import zio.Chunk
import zio.stacktracer.TracingImplicits.disableAutoTrace

import zio.schema.Schema

import zio.http.codec.HttpCodec.Query.QueryParamHint
import zio.http.codec.internal.TextBinaryCodec

private[codec] trait QueryCodecs {

  def query(name: String): QueryCodec[String] = singleValueCodec(name, Schema[String])

  def queryBool(name: String): QueryCodec[Boolean] = singleValueCodec(name, Schema[Boolean])

  def queryInt(name: String): QueryCodec[Int] = singleValueCodec(name, Schema[Int])

  def queryTo[A](name: String)(implicit codec: Schema[A]): QueryCodec[A] = singleValueCodec(name, codec)

  def queryAll(name: String, atLeastOne: Boolean = true): QueryCodec[Chunk[String]] =
    multiValueCodec(name, Schema[String], atLeastOne)

  def queryAllBool(name: String, atLeastOne: Boolean = true): QueryCodec[Chunk[Boolean]] =
    multiValueCodec(name, Schema[Boolean], atLeastOne)

  def queryAllInt(name: String, atLeastOne: Boolean = true): QueryCodec[Chunk[Int]] =
    multiValueCodec(name, Schema[Int], atLeastOne)

  def queryAllTo[A](name: String, atLeastOne: Boolean = true)(implicit codec: Schema[A]): QueryCodec[Chunk[A]] =
    multiValueCodec(name, codec, atLeastOne)

  private def singleValueCodec[A](name: String, schema: Schema[A]): QueryCodec[A] =
    HttpCodec.Query(name, CodecBuilderWithSchema[A](TextBinaryCodec.codecBuilder, schema), QueryParamHint.one)

  private def multiValueCodec[A](name: String, schema: Schema[A], atLeastOne: Boolean): QueryCodec[Chunk[A]] =
    HttpCodec.Query(
      name,
      CodecBuilderWithSchema[A](TextBinaryCodec.codecBuilder, schema),
      if (atLeastOne) QueryParamHint.many else QueryParamHint.any,
    )
}
