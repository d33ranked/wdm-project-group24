from msgspec import msgpack, Struct

class UserValue(Struct):
    credit: int