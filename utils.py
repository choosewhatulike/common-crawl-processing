import os
import gzip

class Writer:
    def __init__(self, output_dir, prefix='file', offset=0, strike=1, max_items=50000):
        self.output_dir = output_dir
        self.prefix = prefix
        self.offset = offset
        self.strike_idx = 0
        self.strike = strike
        self.num_items = 0
        self.max_items = max_items
        self._fp = None
        
    def get_fp(self):
        if self._fp is None:
            output_path = os.path.join(self.output_dir, f'{self.prefix}_{self.strike_idx * self.strike + self.offset}.jsonl.gz')
            self._fp = gzip.open(output_path, 'wt', encoding='utf-8')
        return self._fp
    
    def update_fp(self):
        print('Updating FP')
        if self._fp is not None:
            self._fp.close()
            self.strike_idx += 1
        self._fp = None
        return self.get_fp()
        
    def write_line(self, line):
        fp = self.get_fp()
        fp.write(line)
        fp.write('\n')
        self.num_items += 1
        if self.num_items % self.max_items == 0:
            self.update_fp()
            
if __name__ == '__main__':
    writer = Writer('test', offset=1, max_items=1000000)
    for i in range(1401247):
        writer.write_line(str(i))
