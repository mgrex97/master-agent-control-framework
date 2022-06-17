import netaddr


def _valid_ip(strategy, bits, addr, flags=0):
    addr = addr.split('/')
    if len(addr) == 1:
        return strategy(addr[0], flags)
    elif len(addr) == 2:
        return strategy(addr[0], flags) and 0 <= int(addr[1]) <= bits
    else:
        return False


def valid_ipv4(addr, flags=0):
    """
    Wrapper function of "netaddr.valid_ipv4()".

    The function extends "netaddr.valid_ipv4()" to enable to validate
    IPv4 network address in "xxx.xxx.xxx.xxx/xx" format.

    :param addr: IP address to be validated.
    :param flags: See the "netaddr.valid_ipv4()" docs for details.
    :return: True is valid. False otherwise.
    """
    return _valid_ip(netaddr.valid_ipv4, 32, addr, flags)


def valid_ipv6(addr, flags=0):
    """
    Wrapper function of "netaddr.valid_ipv6()".

    The function extends "netaddr.valid_ipv6()" to enable to validate
    IPv4 network address in "xxxx:xxxx:xxxx::/xx" format.

    :param addr: IP address to be validated.
    :param flags: See the "netaddr.valid_ipv6()" docs for details.
    :return: True is valid. False otherwise.
    """
    return _valid_ip(netaddr.valid_ipv6, 128, addr, flags)
