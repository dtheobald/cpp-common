from _pysnmp import ffi, lib

lib.snmp_setup("rkd")
tbl = lib.create_counter_table("rkd-tbl", ".1.2.3.4")
lib.increment_counter_table(tbl)
lib.delete_counter_table(tbl)
lib.snmp_terminate("rkd")

