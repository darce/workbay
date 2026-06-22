# workbay

Name reservation for the WorkBay front-door installer.

The single-command `workbay` installer is on its way. Until it lands,
install the runtime stack directly:

```sh
pip install workbay-stack
```

`workbay-stack` is a one-number version anchor that pulls every published
WorkBay runtime package. This `workbay` distribution carries no runtime code
yet and will become the single-command front door in a following release.
