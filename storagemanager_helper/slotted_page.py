import struct

PAGE_SIZE = 4096 
HEADER_SIZE = 4
SLOT_SIZE = 8

class SlottedPage:
    def __init__(self):
        self.data = bytearray(PAGE_SIZE)
        self.record_count = 0
        self.free_space_offset = HEADER_SIZE
        self.free_record_offset = PAGE_SIZE
        self.slots = []

    def add_record(self, record_bytes):
        record_length = len(record_bytes)
        
        record_start = self.free_record_offset - record_length
        if record_start < self.free_space_offset + SLOT_SIZE:
            raise Exception("Not enough space to add record")

        self.data[record_start:self.free_record_offset] = record_bytes

        self.data[self.free_space_offset:self.free_space_offset + SLOT_SIZE] = struct.pack('<II', record_start, record_length)

        self.slots.append((record_start, record_length))
        self.free_space_offset += SLOT_SIZE
        self.free_record_offset = record_start
        self.record_count += 1

    
    def serialize(self):
        header = struct.pack("<HH", self.record_count, self.free_space_offset)
        self.data[0:HEADER_SIZE] = header
        return bytes(self.data)
    
    def load(self, byte_data):
        self.data = bytearray(byte_data)
        self.record_count, self.free_space_offset = struct.unpack("<HH", self.data[:HEADER_SIZE])
        self.slots = []
        for i in range(self.record_count):
            offset = HEADER_SIZE + i * SLOT_SIZE
            record_start, record_length = struct.unpack("<II", self.data[offset:offset + SLOT_SIZE])
            self.slots.append((record_start, record_length))
        
        if self.slots:
            self.free_record_offset = min(start for start, _ in self.slots)
        else:
            self.free_record_offset = PAGE_SIZE

    def get_record(self, slot_index):  
        record_start, record_length = self.slots[slot_index]
        return bytes(self.data[record_start:record_start + record_length])
    
    def update_record(self, slot_index, new_record_bytes):
        new_length = len(new_record_bytes)
        old_start, old_length = self.slots[slot_index]

        if new_length == old_length:
            self.data[old_start:old_start + new_length] = new_record_bytes
            return True
        
        if new_length < old_length:
            diff = old_length - new_length
            self.data[old_start:old_start + new_length] = new_record_bytes

            upper_data_start = old_start + new_length
            upper_data_length = self.free_record_offset - upper_data_start
            if upper_data_length > 0:
                self.data[old_start + new_length : old_start + new_length + upper_data_length] = self.data[upper_data_start : upper_data_start + upper_data_length]

            self.free_record_offset += diff

            self.slots[slot_index] = (old_start, new_length)

            for i, (start, length) in enumerate(self.slots):
                if start < old_start:
                    continue
                self.slots[i] = (start + diff, length)

            return True
        
        extra_space_needed = new_length - old_length

        if self.free_record_offset - extra_space_needed < self.free_space_offset + SLOT_SIZE:
            return False
        
        upper_data_start = old_start + old_length
        upper_data_length = self.free_record_offset - upper_data_start
        if upper_data_length > 0:
            self.data[old_start + new_length : old_start + new_length + upper_data_length] = self.data[upper_data_start : upper_data_start + upper_data_length]

        self.data[old_start:old_start + new_length] = new_record_bytes

        self.free_record_offset -= extra_space_needed

        self.slots[slot_index] = (old_start, new_length)

        for i, (start, length) in enumerate(self.slots):
            if start < old_start:
                continue
            self.slots[i] = (start - extra_space_needed, length)

        return True
    
    def delete_record(self, slot_index):
        old_start, old_length = self.slots[slot_index]

        upper_data_length = old_start - self.free_record_offset
        
        if upper_data_length > 0:
            self.data[self.free_record_offset + old_length : old_start + old_length] = self.data[self.free_record_offset : old_start]

        # Update free_record_offset
        self.free_record_offset += old_length

        del self.slots[slot_index]
        self.record_count -= 1
        self.free_space_offset -= SLOT_SIZE

        for i, (start, length) in enumerate(self.slots):
            if start < old_start:  # Records physically above (lower offset)
                self.slots[i] = (start + old_length, length)

        for i, (start, length) in enumerate(self.slots):
            offset = HEADER_SIZE + i * SLOT_SIZE
            self.data[offset:offset + SLOT_SIZE] = struct.pack('<II', start, length)
        
        old_last_slot_offset = HEADER_SIZE + self.record_count * SLOT_SIZE
        self.data[old_last_slot_offset:old_last_slot_offset + SLOT_SIZE] = b'\x00' * SLOT_SIZE




