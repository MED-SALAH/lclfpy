import fastavro.read
import fastavro.write
import fastavro.schema
import fastavro.validation


def schemaless_reader(fo, writer_schema, reader_schema=None, return_record_name=True):
    return fastavro.read.schemaless_reader(fo, writer_schema, reader_schema=reader_schema, return_record_name=return_record_name)

setattr(fastavro, 'schemaless_reader', schemaless_reader)
