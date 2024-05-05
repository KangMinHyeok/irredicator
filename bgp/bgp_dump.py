from mrtparse import *
from datetime import *

class BgpDump:
    __slots__ = [
        'vpName', 'verbose', 'output', 'ts_format', 'pkt_num', 'type', 'num', 'ts',
        'org_time', 'flag', 'peer_ip', 'peer_as', 'nlri', 'withdrawn',
        'as_path', 'origin', 'next_hop', 'local_pref', 'med', 'comm',
        'atomic_aggr', 'aggr', 'as4_path', 'as4_aggr', 'old_state', 'new_state',
    ]

    def __init__(self, vpName='', verbose=False, output=sys.stdout, ts_format='dump', pkt_num=False):
        # self.vpName = args.vpName
        # self.verbose = args.verbose
        # self.output = args.output
        # self.ts_format = args.ts_format
        # self.pkt_num = args.pkt_num
        self.vpName = vpName
        self.verbose = verbose
        self.output = output
        self.ts_format = ts_format
        self.pkt_num = pkt_num
        self.type = ''
        self.num = 0
        self.ts = 0
        self.org_time = 0
        self.flag = ''
        self.peer_ip = ''
        self.peer_as = 0
        self.nlri = []
        self.withdrawn = []
        self.as_path = []
        self.origin = ''
        self.next_hop = []
        self.local_pref = 0
        self.med = 0
        self.comm = ''
        self.atomic_aggr = 'NAG'
        self.aggr = ''
        self.as4_path = []
        self.as4_aggr = ''
        self.old_state = 0
        self.new_state = 0

    def get_routes(self):
        lines = []
        # for withdrawn in self.withdrawn:
        #     if self.type == 'BGP4MP':
        #         self.flag = 'W'
        #     lines.append(self.get_line(withdrawn, ''))
        for nlri in self.nlri:
            if self.type == 'BGP4MP':
                self.flag = 'A'
            for next_hop in self.next_hop:
                lines.append(self.get_line(nlri, next_hop))
        return lines

    def bgp4mp(self, m, count):
        self.type = 'BGP4MP'
        self.ts = datetime.utcfromtimestamp(m['timestamp'][0]).strftime('%Y%m%d')
        
        if (m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_AS4']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
            if m['bgp_message']['type'][0] != BGP_MSG_T['UPDATE']:
                return
            for attr in m['bgp_message']['path_attributes']:
                self.bgp_attr(attr)
            
            lines = []
            for nlri in m['bgp_message']['nlri']:
                prefix = '{}/{}'.format(nlri['prefix'], nlri['prefix_length'])
                if self.type == 'BGP4MP' or self.type == 'BGP4MP_ET':
                    self.flag = 'A'
                
                if self.flag == 'A':
                    lines.append('{}|{}|{}'.format(self.ts, prefix, self.get_origin_as()))

            return lines

        return []
            # return  self.get_routes()

    def bgp_attr(self, attr):
        self.as4_path = []

        if attr['type'][0] == BGP_ATTR_T['ORIGIN']:
            self.origin = ORIGIN_T[attr['value']]
        
        
        elif attr['type'][0] == BGP_ATTR_T['AS_PATH']:
            self.as_path = []
            for seg in attr['value']:
                if seg['type'][0] == AS_PATH_SEG_T['AS_SET']:
                    self.as_path.append('{%s}' % ','.join(seg['value']))
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as_path.append('(' + seg['value'][0])
                    self.as_path += seg['value'][1:-1]
                    self.as_path.append(seg['value'][-1] + ')')
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as_path.append('[%s]' % ','.join(seg['value']))
                else:
                    self.as_path += seg['value']
        
        elif attr['type'][0] == BGP_ATTR_T['AS4_PATH']:
            for seg in attr['value']:
                if seg['type'][0] == AS_PATH_SEG_T['AS_SET']:
                    self.as4_path.append('{%s}' % ','.join(seg['value']))
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as4_path.append('(' + seg['value'][0])
                    self.as4_path += seg['value'][1:-1]
                    self.as4_path.append(seg['value'][-1] + ')')
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as4_path.append('[%s]' % ','.join(seg['value']))
                else:
                    self.as4_path += seg['value']
        
    def merge_as_path(self):
        if len(self.as4_path):
            n = len(self.as_path) - len(self.as4_path)
            return ' '.join(self.as_path[:n] + self.as4_path)
        else:
            return ' '.join(self.as_path)

    def get_origin_as(self):
        origin = ''
        if len(self.as4_path) > 0:
            origin = self.as4_path[-1]
        elif len(self.as_path):
            origin = self.as_path[-1]

        return origin

    def merge_aggr(self):
        if len(self.as4_aggr):
            return self.as4_aggr
        else:
            return self.aggr

