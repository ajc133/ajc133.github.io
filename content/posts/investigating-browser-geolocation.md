---
title: How is location shared without using GPS?
date: 2024-10-11
description: Investigating Firefox’s Geolocation Feature
categories: []
tags: []
---

{{< figure src="/allow-access-question.png" alt="Browser pop-up asking if I'd like to allow google.com to access my location" >}}

How does location sharing work when my computer doesn't have GPS?

If I open my laptop to Google Maps and allow it to access my location, it returns my location to within 20 meters!
How can that possibly work without GPS?

Clicking [Learn more](https://www.mozilla.org/en-US/firefox/geolocation/) in the above pop-up tells me (emphasis mine):

> By default, Firefox uses Google Location Services to determine your location by sending:

- your computer’s **IP address**,
- information about the **nearby wireless access points**, and

## My computer's IP address

Using IP addresses for geolocation is normally accurate to within the same city, you can read more about [IP address geolocation here](https://whatismyipaddress.com/geolocation).

You can look up your own IP address on, for example, [IPLocation.net](https://www.iplocation.net/ip-lookup), which gives you results from multiple services.
If you look up your IP address, you may notice that not all the results are accurate, so obviously this wouldn't work well as a method for pinpointing your precise location.

## Nearby wireless access points

This refers to the various WiFi networks that my computer can see.
So high accuracy geolocation comes from WiFi!

**How does Google determine location from WiFi??**

Google has been collecting data on WiFi networks for years.
It [stated back in 2010](http://static.googleusercontent.com/media/www.google.com/en//googleblogs/pdfs/google_submission_dpas_wifi_collection.pdf) that it uses Google Street View cars to collect WiFi data in our neighborhoods.
They store the WiFi networks they detect + the car’s GPS coordinates for future use.

{{< figure src="/google-street-view-car.webp" title="Peeping on your WiFi" alt="A parked street view car with its massive camera strapped to the roof" >}}

So when Firefox sends Google my nearby WiFi networks, Google looks up where it last saw that combination of WiFi networks (which was in my neighborhood) and replies with the coordinates it stored.

# Inspecting the data that Firefox is sending to Google

## Using Developer Tools

I want to see if I can intercept the request that Firefox sends to Google.
So I set up a basic webpage that uses Javascript to request the user’s location from the [Geolocation API](https://developer.mozilla.org/en-US/docs/Web/API/Geolocation_API) and display it on a map.

{{< figure src="/dev-tools-screenshot.png" title="Where are my geolocation requests?" alt="Developer Tools screenshot showing no http requests to any API after clicking 'Allow' on the Location Services pop-up" >}}

Ok, so it turns out that Firefox doesn't record those geolocation API requests in the developer tools.

## Using an HTTP(s) Proxy

I set up [mitmproxy](https://mitmproxy.org/) to intercept the http(s) requests that Firefox was sending.
This is configurable from within Firefox, so we have to trust Firefox to send all of its requests through the proxy.

{{< figure src="/mitm-proxy-screenshot.png" alt="List of http requests that mitmproxy captured from my session with the map page" >}}

Still, no requests to Google’s API show up after clicking “Allow” on the location popup.
Only requests to my site, map providers, and general Firefox telemetry show up.
Does Firefox not respect the http(s) proxy that it’s configured to use?
Wouldn’t it break in corporate environments where the app must use the proxy?

## Using Wireshark

I recently learned how to enable Wireshark [to decrypt TLS traffic](https://wiki.wireshark.org/TLS#tls-dissection-in-wireshark).
I prefer to do it the easy way by setting SSLKEYLOGFILE when opening Firefox.
I recommend creating a new [Firefox profile](https://support.mozilla.org/en-US/kb/profile-manager-create-remove-switch-firefox-profiles) if you're going to try this, you don't want to accidentally capture your own sensitive data.

I launch a new browser window with the separate profile and `SSLKEYLOGFILE` environment variable set

```bash
SSLKEYLOGFILE="$HOME/decrypted.log" firefox -P "Decrypted" &
```

Then I visit my map, start the capture, click the *Allow* button for the pop-up, wait until it shows my location, stop the capture and **VOILA**!

{{< figure src="/wireshark-geolocate-decrypted-1.png" alt="Wireshark session showing the decrypted http2 geolocation request" >}}

There's the geolocation POST request with all of my nearby WiFi networks!
But why is it going to... `location.services.mozilla.com`??

{{< figure src="/geolocate-post-request-redacted.png" alt="POST request with all of my nearby access point MAC addresses in the body" >}}

What's more confusing, it failed this time with 404 NOT FOUND.

{{< figure src="/geolocate-response-404.png" alt="POST response saying 404 not found" >}}

I reopened the webpage while running Wireshark, and this time it successfully showed my location.
However, there's no corresponding HTTP request...
A bit more investigation shows that it's an HTTP3 request, which I know very little about.

{{< figure src="/geolocate-http3-request.png" alt="Wireshark screenshot showing HTTP/3 packets" >}}

Wireshark doesn't show me any interesting info in the HTTP/3 packets, sadly, because it turns out that Wireshark [still hasn't implemented HTTP/3 support](https://gitlab.com/wireshark/wireshark/-/issues/16761).
I'm looking forward to the day that this feature gets released!

I had fun learning about how geolocation works using WiFi.
It was also satisfying to learn new tools like mitmproxy and learning how to decrypt TLS with Wireshark!

