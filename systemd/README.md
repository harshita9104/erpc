# systemd configuration

This directory contains erpc systemd service units to run erpc as a daemon.

## To run the `user` unit in a Debian system:

```bash
cp user/erpc.service /path_to_erpc_user_home/.config/systemd/user`
```

Replace anything containing `_` in these commands and the files with the
suitables values for your system.

To start the service on boot run, `loginctl enable-linger erpc_user`

Reload systemd user daemon: `systemctl --user daemon-reload`

To enable and start the service now, as erpc_user run:
`systemctl --user enable --now erpc.service`
