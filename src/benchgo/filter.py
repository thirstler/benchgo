import random

class Filter:

    def get_rnd_ks_slice(self, ratio:float=1.0, bitwidth:int=32, ret_float=False):

        while True:
            target_length = (2**bitwidth) * ratio
            
            if ret_float:
                target_offset = random.random() * random.randrange(-2**(bitwidth-1), 2**(bitwidth-1))
            else:
                target_offset = int(random.random() * random.randrange(-2**(bitwidth-1), 2**(bitwidth-1)))

            # Keep trying until you get a slice that doesn't exceed the bounds
            if target_offset + target_length < (2**(bitwidth-1)):
                if ret_float:
                    return target_offset, (target_offset+target_length)
                else:
                    return target_offset, int(target_offset+target_length)
    