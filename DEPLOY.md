# Deployment

I'm setting up a VM at Hetzner for the webapp. I haven't hosted anything there before, I just want to try
hosting my things with european owners for the foreseeable future. This is my notes to myself about
deployment and ops.

## Object storage

I clickopsed an object storage bucket. Not much to say about this, it needs a name. Pricing is by GB-month
stored and TB egress. The minimum price of ~6 euro covers 1TB-month and 1TB egress, which should be plenty.

I used the minio client `mc` to upload files. I needed to first write a `~/.mc/config.json`. The documentation
in the Hetzner portal got me started. 

I can set cors config like this:
```shell
mc cors set bus-eta/kaaveland-bus-eta-data s3_cors_config.xml
```

The cors config I've set enable `Access-Control-Allow-Origin: *` and `Access-Control-Allow-Methods: GET,HEAD`.

I can upload new static data files like this:

```shell
mc cp stop_line.parquet bus-eta/kaaveland-bus-eta-data/stop_line.parquet
mc cp leg_stats.parquet bus-eta/kaaveland-bus-eta-data/leg_stats.parquet
mc cp stop_stats.parquet bus-eta/kaaveland-bus-eta-data/stop_stats.parquet
```

## Server

I clickopsed a server too. I set it up with a firewall that permits port 22 only from my IP,
but port 80 and 443 from the rest of the world. I won't be able to ssh from everywhere,
but if something's wrong I can sign into the Hetzner cloud console and add another IP to
whitelist.

I selected ubuntu 24.04.2. I went with a shared CPU machine. It looks easy to change to a
dedicated one if I change my mind. This machine is about 7 euro a month. I plan to move 
some things from an american cloud vendor to this server too. If the server doesn't have
terrible noisy neighbour-problems, it is a very reasonable price.

The first thing I did was to upgrade all packages:

```shell
apt update && apt upgrade -y 
```

I needed to reboot it to get a new kernel:
```shell
reboot 0
```

Really, I should be doing absolutely everything from this point on using ansible or puppet,
but I'll deal with that later. Instead, I'm making this meticulous log of the session so 
that I _can_ easily port it when I find the time.

### Unattended upgrades

I'm not going to remember to do this as a routine thing, so I have to automate it.

Unattended upgrades was already installed by default:

```shell
unattended-upgrades/noble,now 2.9.1+nmu4ubuntu1 all [installed]
  automatic installation of security upgrades
```

But I need the machine to restart now and then. 

I can configure things in `/etc/apt/apt.conf.d/50unattended-upgrades`. I found and uncommented these
lines:

```
// Automatically reboot *WITHOUT CONFIRMATION* if
//  the file /var/run/reboot-required is found after the upgrade
Unattended-Upgrade::Automatic-Reboot "false";

// Automatically reboot even if there are users currently logged in
// when Unattended-Upgrade::Automatic-Reboot is set to true
Unattended-Upgrade::Automatic-Reboot-WithUsers "true";

// If automatic reboot is enabled and needed, reboot at the specific
// time instead of immediately
//  Default: "now"
Unattended-Upgrade::Automatic-Reboot-Time "02:00";

// Enable logging to syslog. Default is False
Unattended-Upgrade::SyslogEnable "false";
```

This seems like it should do what I want.

This command tells me that the unattended-upgrades package is enabled:

```shell
systemctl status unattended-upgrades
```

Now I just need to make sure that whatever I set up on the machine comes up
automatically, which probably just means it needs to be enabled in `systemctl`.

### nginx and certbot

I was considering to use something like [caddy](https://caddyserver.com/), but I'm already
familiar with `nginx` and `certbot`, so it's where I'll start. I am not exposing any 
services to the internet without making them go through a reverse proxy. I am going
to choose to trust the versions that are in `apt`:

```shell
apt install nginx certbot python3-certbot-nginx -y
```

Next, I need a server block for my domain. This is for `/etc/nginx/sites-available/kollektivkart.conf`:

```
server {
    server_name kollektivkart.kaveland.no kollektivkart.arktekk.no;
}
```

After writing it, I can run this command to enable it:

```shell
ln -s /etc/nginx/{sites-available,sites-enabled}/kollektivkart.conf
systemctl reload nginx
```

This is enough to get certbot started:

```shell
certbot --nginx -d kollektivkart.kaveland.no -d kollektivkart.arktekk.no
```

It prompts me for email address and accept the terms, it installs the certificate.

I remove the default site:

```shell
rm /etc/nginx/sites-enabled/default
```

I check out `/etc/nginx/sites-available/kollektivkart.conf` and notice that
certbot has taken care of automatic promotion to TLS and setting up some cipher suits and a 
server block. Nice. I run the [ssltest](https://www.ssllabs.com/ssltest/)  and it gets an A.

I run `grep -R certbot /etc/cron*` and I see a job for automatic renewal. Nice.

#### Configuring nginx as a reverse proxy

I _think_ the default nginx config on ubuntu is reasonably good. But I add some extra precations
to `/etc/nginx/conf.d/reverse_proxy.conf` anyway:

```
add_header X-Frame-Options "SAMEORIGIN";
add_header X-XSS-Protection "1; mode=block";
add_header X-Content-Type-Options "nosniff";
client_max_body_size 1M;
limit_req_zone $binary_remote_addr zone=request_limit:10m rate=15r/s;
limit_conn_zone $binary_remote_addr zone=conn_limit_per_ip:10m;
client_header_buffer_size 4k;
large_client_header_buffers 2 4k;
keepalive_timeout 15s;
client_body_timeout 15s;
client_header_timeout 15s;
proxy_connect_timeout 5s;
proxy_read_timeout 5s;

gzip_vary on;
gzip_proxied any;
gzip_comp_level 6;
gzip_buffers 16 8k;
gzip_http_version 1.1;
gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss text/javascript;
```

I check the syntax with `nginx -t` and reload with `systemctl reload nginx` to apply.

The idea is to set appropriate security headers and limit how much damage a mean client can do. These
settings will be included into the `http`-block in `/etc/nginx/nginx.conf` and are global. I'm going to
do some basic rate-limiting later, that's why there's the `limit_req_zone` and `limit_conn_zone`.
But that will go under the `server-block`, once I've deployed the webapp.

I also make sure to add `gzip` support for `Content-Encoding` to save bandwidth.

## Deploying with `docker-compose`

I'm going to set up `docker` on the server. I'm going to follow the guide for installing the
[docker engine](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) with `apt`.

This list of installs:

```shell
 apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
 ```

Appears to have given me everything I need. I don't want to run docker as `root`, so I 
follow the [post installation steps](https://docs.docker.com/engine/install/linux-postinstall/).

I create a user to run the webapp:

```
useradd -m -G docker -s /bin/bash kollektivkart
```

I switch users and verify that I can get the app up:

```shell
kollektivkart@ardbeg:~$ docker run --rm ghcr.io/kaaveland/bus-eta
```

Everything seems fine, so I just put the `docker-compose.yml` from this repository there. I will try to run it
with `docker compose up -d` at first. Later I will need to set up systemd configuration.

### Fixing `docker-compose.yml`

My docker compose file was all wrong. I do _not_ want to expose `gunicorn` directly to the internet. So I had
to fix the `ports:`-section to expose only on the loopback interface:

```yaml
    ports:
      - "127.0.0.1:8000:8000"
```

The memory limit was also too low. I increased it to `1024m`. The server has plenty, at 8GB.

### Configuring the reverse-proxy

I need to set up the `server`-block in `/etc/sites-available/kollektivkart.conf` correctly now.
Inside the `server` block where certbot has added `listen 443 ssl`, I add:

```
    location = /robots.txt {
        add_header Content-Type text/plain;
        return 200 "User-agent: *\nDisallow: /";
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
	    proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
	    limit_conn conn_limit_per_ip 20;
	    limit_req zone=request_limit burst=20 nodelay;
    }
```

I don't want this page to be scanned by bots since almost all the content is dynamic.

At this point, I have [kollektivkart.arktekk.no](https://kollektivkart.arktekk.no/) working!

### Systemd setup

TODO: I need the docker container to be managed by systemd, so it automatically starts on boot.

Will take care of that tomorrow.