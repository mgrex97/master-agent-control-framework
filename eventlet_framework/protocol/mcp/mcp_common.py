from struct import calcsize

# Machine Service Header
MCP_HEADER_PACK_STR = '!HHII'
MCP_HEADER_SIZE = 12

assert calcsize(MCP_HEADER_PACK_STR) == MCP_HEADER_SIZE
