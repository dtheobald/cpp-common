from _pysnmp import ffi, lib


class CounterTable(object):
    def __init__(self, name, oid):
        self.tbl = lib.create_counter_table(name, oid)

    def __del__(self):
        if lib is not None:
            lib.delete_counter_table(self.tbl)

    def increment(self):
        lib.increment_counter_table(self.tbl)

lib.snmp_setup("rkd")
t = CounterTable("rkd-tbl", ".1.2.3.4")
t.increment()
lib.snmp_terminate("rkd")

