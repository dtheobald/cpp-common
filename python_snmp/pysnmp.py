# file "example_build.py"

from cffi import FFI
ffi = FFI()

ffi.set_source("_pysnmp",
               """ // passed to the real C compiler
               #include "snmp_agent.h"
               #include "snmp_c_linkage.h"
               """,
               libraries=["pysnmp"])   # or a list of libraries to link with

ffi.cdef("""     // some declarations from the man page
         int snmp_setup(const char* name);
         void snmp_terminate(const char* name);
         void* create_counter_table(const char* name, const char* oid);
         void delete_counter_table(void* table);
         void increment_counter_table(void* table);
         """)

if __name__ == "__main__":
    ffi.compile()
