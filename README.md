# TPX Pipeline

Simple daemon to live-convert TPX3 data from Serval.

## Run

Option 1: import and boot from Python

- Import `src.tpx_pipe_ioc`
- Call `test_boot()`
- Use the returned pipeline trigger and output queues

Option 2: run as a fake EPICS IOC

From the project root:

```bash
pixi run python -m src.tpx_pipe_ioc {alternate output directory}
```