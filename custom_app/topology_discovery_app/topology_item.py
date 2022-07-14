from collections import defaultdict
import time


class Port(object):
    # This is data class passed by EventPortXXX
    def __init__(self, mgr_address, port_no, hw_addr, info=None):
        super(Port, self).__init__()

        self.mgr_address = mgr_address
        self.port_no = port_no
        self.hw_addr = hw_addr
        self.info = info

    def is_down(self):
        pass

    def is_live(self):
        pass

    def to_dict(self):
        return {'mgn_addr': self.mgr_address,
                'port_no': self.port_no,
                'hw_addr': self.hw_addr,
                'info': self.info}

    # for Switch.del_port()
    def __eq__(self, other):
        return self.hw_addr == other.hw_addr and self.port_no == other.port_no

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.hw_addr, self.port_no))

    def __str__(self):
        # LIVE_MSG = {False: 'DOWN', True: 'LIVE'}
        return 'Port<SW=%s, MAC: %s, port_no=%s>' % \
            (self.mgr_address, self.hw_addr,
             self.port_no)  # , LIVE_MSG[self.is_live()])


class Switch(object):
    def __init__(self, mgr_addr, hw_addr, name=None, info=None):
        super(Switch, self).__init__()
        self.mgr_addr = mgr_addr
        self.hw_addr = hw_addr
        self.name = name
        self.ports = {}
        self.info = info

    def add_port(self, port_no, Port: Port):
        self.ports[port_no] = Port

    def del_port(self, port_no):
        self.ports.pop(port_no)

    def to_dict(self):
        d = {'MAC': (self.hw_addr),
             'ports': [port.to_dict() for port in self.ports]}
        return d

    def __str__(self):
        msg = f'Switch <name={self.name}, MAC={self.hw_addr}, Ports: '
        for port in self.ports:
            msg += str(port) + ' ,'

        msg += '>'
        return msg


class Link(object):
    # This is data class passed by EventLinkXXX
    def __init__(self, src, dst):
        super(Link, self).__init__()
        self.src = src
        self.dst = dst

    def to_dict(self):
        d = {'src': self.src.to_dict(),
             'dst': self.dst.to_dict()}
        return d

    # this type is used for key value of LinkState
    def __eq__(self, other):
        return self.src == other.src and self.dst == other.dst

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.src, self.dst))

    def __str__(self):
        return 'Link: %s to %s' % (self.src, self.dst)


class Host(object):
    # This is data class passed by EventHostXXX
    def __init__(self, mac, port):
        super(Host, self).__init__()
        self.port = port
        self.mac = mac
        self.ipv4 = []
        self.ipv6 = []

    def to_dict(self):
        d = {'mac': self.mac,
             'ipv4': self.ipv4,
             'ipv6': self.ipv6,
             'port': self.port.to_dict()}
        return d

    def __eq__(self, host):
        return self.mac == host.mac and self.port == host.port

    def __str__(self):
        msg = 'Host<mac=%s, port=%s,' % (self.mac, str(self.port))
        msg += ','.join(self.ipv4)
        msg += ','.join(self.ipv6)
        msg += '>'
        return msg


class HostState(dict):
    # mac address -> Host class
    def __init__(self):
        super(HostState, self).__init__()

    def add(self, host):
        mac = host.mac
        self.setdefault(mac, host)

    def update_ip(self, host, ip_v4=None, ip_v6=None):
        mac = host.mac
        host = None
        if mac in self:
            host = self[mac]

        if not host:
            return

        if ip_v4 is not None:
            if ip_v4 in host.ipv4:
                host.ipv4.remove(ip_v4)
            host.ipv4.append(ip_v4)

        if ip_v6 is not None:
            if ip_v6 in host.ipv6:
                host.ipv6.remove(ip_v6)
            host.ipv6.append(ip_v6)

    def get_by_dpid(self, dpid):
        result = []

        for mac in self:
            host = self[mac]
            if host.port.dpid == dpid:
                result.append(host)

        return result


class PortState(dict):
    # dict: int port_no -> OFPPort port
    # OFPPort is defined in ryu.ofproto.ofproto_v1_X_parser
    def __init__(self):
        super(PortState, self).__init__()

    def add(self, port_no, port):
        self[port_no] = port

    def remove(self, port_no):
        del self[port_no]

    def modify(self, port_no, port):
        self[port_no] = port


class PortData(object):
    def __init__(self, is_down, lldp_data):
        super(PortData, self).__init__()
        self.is_down = is_down
        self.lldp_data = lldp_data
        self.timestamp = None
        self.sent = 0

    def lldp_sent(self):
        self.timestamp = time.time()
        self.sent += 1

    def lldp_received(self):
        self.sent = 0

    def lldp_dropped(self):
        return self.sent

    def clear_timestamp(self):
        self.timestamp = None

    def set_down(self, is_down):
        self.is_down = is_down

    def __str__(self):
        return 'PortData<live=%s, timestamp=%s, sent=%d>' \
            % (not self.is_down, self.timestamp, self.sent)


class PortDataState(dict):
    # dict: Port class -> PortData class
    # slimed down version of OrderedDict as python 2.6 doesn't support it.
    _PREV = 0
    _NEXT = 1
    _KEY = 2

    def __init__(self):
        super(PortDataState, self).__init__()
        self._root = root = []  # sentinel node
        root[:] = [root, root, None]  # [_PREV, _NEXT, _KEY] doubly linked list
        self._map = {}

    def _remove_key(self, key):
        link_prev, link_next, key = self._map.pop(key)
        link_prev[self._NEXT] = link_next
        link_next[self._PREV] = link_prev

    def _append_key(self, key):
        root = self._root
        last = root[self._PREV]
        last[self._NEXT] = root[self._PREV] = self._map[key] = [last, root,
                                                                key]

    def _prepend_key(self, key):
        root = self._root
        first = root[self._NEXT]
        first[self._PREV] = root[self._NEXT] = self._map[key] = [root, first,
                                                                 key]

    def _move_last_key(self, key):
        self._remove_key(key)
        self._append_key(key)

    def _move_front_key(self, key):
        self._remove_key(key)
        self._prepend_key(key)

    def add_port(self, port, lldp_data):
        if port not in self:
            self._prepend_key(port)
            self[port] = PortData(port.is_down(), lldp_data)
        else:
            self[port].is_down = port.is_down()

    def lldp_sent(self, port):
        port_data = self[port]
        port_data.lldp_sent()
        self._move_last_key(port)
        return port_data

    def lldp_received(self, port):
        self[port].lldp_received()

    def move_front(self, port):
        port_data = self.get(port, None)
        if port_data is not None:
            port_data.clear_timestamp()
            self._move_front_key(port)

    def set_down(self, port):
        is_down = port.is_down()
        port_data = self[port]
        port_data.set_down(is_down)
        port_data.clear_timestamp()
        if not is_down:
            self._move_front_key(port)
        return is_down

    def get_port(self, port):
        return self[port]

    def del_port(self, port):
        del self[port]
        self._remove_key(port)

    def __iter__(self):
        root = self._root
        curr = root[self._NEXT]
        while curr is not root:
            yield curr[self._KEY]
            curr = curr[self._NEXT]

    def clear(self):
        for node in self._map.values():
            del node[:]
        root = self._root
        root[:] = [root, root, None]
        self._map.clear()
        dict.clear(self)

    def items(self):
        'od.items() -> list of (key, value) pairs in od'
        return [(key, self[key]) for key in self]

    def iteritems(self):
        'od.iteritems -> an iterator over the (key, value) pairs in od'
        for k in self:
            yield (k, self[k])


class LinkState(dict):
    # dict: Link class -> timestamp
    def __init__(self):
        super(LinkState, self).__init__()
        self._map = defaultdict(lambda: defaultdict(lambda: None))

    def get_peers(self, src):
        return self._map[src].keys()

    def update_link(self, src, dst):
        link = Link(src, dst)

        self[link] = time.time()
        self._map[src][dst] = link

        # return if the reverse link is also up or not
        rev_link = Link(dst, src)
        return rev_link in self

    def link_down(self, link):
        del self[link]
        del self._map[link.src][link.dst]

    def rev_link_set_timestamp(self, rev_link, timestamp):
        # rev_link may or may not in LinkSet
        if rev_link in self:
            self[rev_link] = timestamp

    def port_deleted(self, src):
        dsts = self.get_peers(src)

        rev_link_dsts = []
        for dst in dsts:
            link = Link(src, dst)
            rev_link = Link(dst, src)
            del self[link]
            self.pop(rev_link, None)
            if src in self._map[dst]:
                del self._map[dst][src]
                rev_link_dsts.append(dst)

        del self._map[src]
        return dsts, rev_link_dsts
