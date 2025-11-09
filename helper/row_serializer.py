from data_encoder import DataEncoder

class RowSerializer:
    def __init__(self):
        self.encoder = DataEncoder()

    def serialize(self, schema, record):
        byte_array = bytearray()
        for field in schema:
            field_name = field['name']
            field_type = field['type']
            if field_type == 'int':
                byte_array += self.encoder.encode_int(record[field_name])
            elif field_type == 'float':
                byte_array += self.encoder.encode_float(record[field_name])
            elif field_type == 'char':
                length = field['length'] # Can be changed depends on schema design
                byte_array += self.encoder.encode_char(record[field_name], length)
            elif field_type == 'varchar':
                max_length = field['max_length'] # Can be changed depends on schema design
                byte_array += self.encoder.encode_varchar(record[field_name], max_length)
        return bytes(byte_array)

    def deserialize(self, schema, byte_data):
        record = {}
        offset = 0
        for field in schema:
            field_name = field['name']
            field_type = field['type']
            if field_type == 'int':
                value, offset = self.encoder.decode_int(byte_data, offset)
            elif field_type == 'float':
                value, offset = self.encoder.decode_float(byte_data, offset)
            elif field_type == 'char':
                length = field['length'] # Can be changed depends on schema design
                value, offset = self.encoder.decode_char(byte_data, offset, length)
            elif field_type == 'varchar': # Can be changed depends on schema design
                value, offset = self.encoder.decode_varchar(byte_data, offset)
            record[field_name] = value
        return record