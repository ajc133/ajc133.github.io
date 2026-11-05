---
title: "curl with OAuth2"
date: 2026-11-04T08:51:55-08:00
draft: true
---

TIL that [curl](https://curl.se/) has a `--oauth2-bearer` flag.

I used to interpolate my token into the `Authorization:` header like this:

```sh
curl -H "Authorization: Bearer $(auth-cli get-token)"
```

but I think it's nicer to use the builtin flag :)

```sh
curl --oauth2-bearer $(auth-cli get-token)
```

That being said, please use a proper programming language to interact with apps backed by OAuth as soon as you can.

A long time ago, when I was newer to programming, I would read docs that had [example code written only in bash](https://developer.jamf.com/jamf-pro/docs/jamf-pro-api-overview) to interact with their API, so I'd follow suit and explore their API.
It really slowed me down dealing with the shell syntax, confusing string interpolation rules ("Do I quote this subcommand or do I not"), and other common pitfalls.
Once I switched to Python to create my integrations, things went much more smoothly.
