# Data Guide Cache

**Fingerprint**: `a423740bcbce83d4`  
**Paper**: The employment content of EU exports: An application of FIGARO tables  
**Reference year**: 2010  
**Datasets**: io_table, satellite_account  

## Files covered

- `io_table`: data/raw/naio_10_fcp_ip1.tsv.gz
- `satellite_account`: data/raw/nama_10_a64_e.tsv.gz

## Usage

Any pipeline run whose manifest produces fingerprint `a423740bcbce83d4` will automatically load this guide instead of calling the LLM.

To share: `git add data_guides/ && git commit -m 'cache: add data guide a423740bcbce83d4' && git push`
