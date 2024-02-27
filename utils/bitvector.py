import sys

def make_bit_vector(bit_size):
    fill = 0
    num_records = bit_size >> 3                   # number of 8 bit integers
    if (bit_size & 7):                      # if bitSize != (32 * n) add
        num_records += 1                        #    a record for stragglers

    bitArray = [0] * num_records
    return bitArray

def shift_bit_vector(bitvector, delay, bit_size=3650, bitval=0):
    new_vector = make_bit_vector(bit_size)
    for i in range(bit_size):
        if bitval == 1 and i < delay: set_bit(new_vector, i)
        if i + delay > bit_size: break
        if testBit(bitvector, i): set_bit(new_vector, i + delay)
    return new_vector


# testBit() returns a nonzero result, 2**offset, if the bit at 'bit_num' is set to 1.
def test_bit(bitvector, bit_num):
    record = bit_num >> 3
    offset = bit_num & 7
    mask = 1 << offset
    return(bitvector[record] & mask)

# setBit() returns an integer with the bit at 'bit_num' set to 1.
def set_bit(bitvector, bit_num):
    record = bit_num >> 3
    offset = bit_num & 7
    mask = 1 << offset
    bitvector[record] |= mask
    return(bitvector[record])

# clearBit() returns an integer with the bit at 'bit_num' cleared.
def clear_bit(bitvector, bit_num):
    record = bit_num >> 3
    offset = bit_num & 7
    mask = ~(1 << offset)
    bitvector[record] &= mask
    return(bitvector[record])

# toggleBit() returns an integer with the bit at 'bit_num' inverted, 0 -> 1 and 1 -> 0.
def toggle_bit(bitvector, bit_num):
    record = bit_num >> 3
    offset = bit_num & 7
    mask = 1 << offset
    bitvector[record] ^= mask
    return(bitvector[record])

def print_bit(bitvector):
    for record in bitvector:
        for offset in range(8):
            mask = 1 << offset
            if record & mask:
                sys.stdout.write('1')
            else:
                sys.stdout.write('0')
    sys.stdout.write('\n')

def bitvector2str(bitvector):
    if len(bitvector) == 0: return ''

    intstr = ''
    for record in bitvector:
       intstr += str(record) + "|" 

    return intstr[:-1]

def tobitvector(string):
    bitvector = string.split("|")
    return list(map(int, bitvector))
