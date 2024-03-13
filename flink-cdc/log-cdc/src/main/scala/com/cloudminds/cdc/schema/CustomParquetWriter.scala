package com.cloudminds.cdc.schema

import org.apache.flink.formats.parquet.ParquetBuilder
import org.apache.flink.formats.parquet.ParquetWriterFactory
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.io.OutputFile
import java.io.IOException
//import flink.note.SchemaGetter
object CustomParquetWriter {
  /**
   * Creates a ParquetWriterFactory that accepts and writes Avro generic types.
   * The Parquet writers will use the given schema to build and write the columnar data.
   *
   * @param schemaServiceURL The URL of avroSchema.
   */
//  def forStringToGenericRecord(schemaServiceURL: String): ParquetWriterFactory[GenericRecord] = {
  def forStringToGenericRecord(schemaString: String): ParquetWriterFactory[GenericRecord] = {
    val builder = new ParquetBuilder[GenericRecord] {
      override def createWriter(out: OutputFile): ParquetWriter[GenericRecord] = {

        // 这里进行动态获取 schema 的操作
//        val schemaString = SchemaGetter.getAvroSchema(schemaServiceURL)
        createAvroParquetWriter(schemaString, GenericData.get, out)
      }
    }
    new ParquetWriterFactory[GenericRecord](builder)
  }
  @throws[IOException]
  private def createAvroParquetWriter[T](
                                          schemaString: String,
                                          dataModel: GenericData,
                                          out: OutputFile
                                        ): ParquetWriter[T] = {
    val schema: Schema = new Schema.Parser().parse(schemaString)
    AvroParquetWriter
      .builder[T](out)
      .withSchema(schema)
      .withDataModel(dataModel)
      .build()
  }
}
