# TPX Pipeline

Simple daemon to live-convert TPX3 data from Serval.

## Run
for most of these options, to boot with a tpx3 awkward configuration file it must be in the current directory with name 'tpx3.json'.
It can be live changed in the ioc just like the output directory

Option 1: import and boot from Python

- Import `tpx_pipeline.ioc`
- Call `test_boot()`
- Use the returned pipeline trigger and output queues

Option 2: run as a fake EPICS IOC

From the project root:
```bash
pixi run python -m tpx3_pipeline.ioc [--path {alternate output directory} ]  [--prefix {IOC prefix}]
```

Option 3: run using local_dispatch

```bash
pixi run python local_dispatch.py
```
