import random
from datetime import datetime

class Filter:

    def get_rnd_ks_slice(self, ratio:float=1.0, bitwidth:int=32, ret_float=False):
        
        tries = 0
        target_length = (2**bitwidth) * ratio
        if target_length >= 2**bitwidth:
            target_length = (2**bitwidth)-1
            
        while True:
            
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
            
            if tries > 100:
                end = -2**(bitwidth-1) + target_length
                return -2**(bitwidth-1), end if ret_float else int(end)

            tries += 1


    def print_prometheus_stats(self, then, now) -> None:

        try:
            self.prometheus_handler.gather(then, now)
        except:
            print("performance stats not available")
            return

        load_stats = "{t_cluster_util},{v_cluster_util},{agg_cpu_util},{tnet_quiet_in:.2f},{disk_r},{disk_w}".format(
            t_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.exec_cluster_rate) if self.prometheus_handler.collection_data.exec_cluster_rate <= 1 else "",
            v_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.cnode_cluster_rate) if self.prometheus_handler.collection_data.cnode_cluster_rate <= 1 else "",
            agg_cpu_util="{:.2f}".format(
                (   (self.prometheus_handler.collection_data.exec_cluster_rate  * self.prometheus_handler.collection_data.exec_cpus) +
                    (self.prometheus_handler.collection_data.cnode_cluster_rate * self.prometheus_handler.collection_data.cnode_cpus)
                ) / (self.prometheus_handler.collection_data.ttl_cpus if self.prometheus_handler.collection_data.ttl_cpus > 0 else 1)
                ) if (self.prometheus_handler.collection_data.exec_cluster_rate <=1 and self.prometheus_handler.collection_data.cnode_cluster_rate <= 1) else "",
            tnet_quiet_in=self.prometheus_handler.collection_data.exec_net_quiet_in,
            disk_r=self.prometheus_handler.collection_data.exec_disk_r,
            disk_w=self.prometheus_handler.collection_data.exec_disk_w)

        print(load_stats)

    def header(self):

        print("\njob, current_time, timing, rows_returned, exec_cluster_util, storage_cluster_util, aggregate_util, network_in, network_out, disk_read, disk_write")


    def print_get_by_substr(self, strings:dict, col:str):

        for string in strings:
            print("select_by_substr('{}'),".format(string), end="", flush=True)
            then = datetime.now()
            print("{},".format(then), end="", flush=True)

            # Do the thing!
            timing, row_count = self.select_by_substr(string, col, use_sql=True)

            print("{:.4f},".format(timing), end="")
            print("{},".format(row_count), end="")
            self.print_prometheus_stats(then, datetime.now())
                

    def print_select_numeric(self, ratios:dict, bitwidth:int, field:str, sel_float=False):

        for slice in ratios:
            print("select_numeric({}, {}%),".format(field, slice*100), end="", flush=True)
            then = datetime.now()
            print("{},".format(then), end="", flush=True)

            # Do the thing!
            timing, row_count = self.select_numeric(ratio=slice, bitwidth=bitwidth, select_col=field, use_sql=False, sel_float=sel_float)

            print("{:.4f},".format(timing), end="")
            print("{},".format(row_count), end="")
            self.print_prometheus_stats(then, datetime.now())
            

    def print_select_by_words(self, words, column):

        then = datetime.now()
        print("select_by_words('{}'),".format(len(words)), end="", flush=True)
        then = datetime.now()
        print("{},".format(then), end="", flush=True)
        
        self.select_by_words(words, column)
        timing, row_count = self.select_by_words(words, column)

        print("{:.4f},".format(timing), end="")
        print("{},".format(row_count), end="")
        self.print_prometheus_stats(then, datetime.now())