FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3

RUN apt-get update && apt-get install --no-install-recommends -y \
      squid-openssl=6.14-0ubuntu0.24.04.4 python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
       -subj /CN=unused-proxy-certificate -keyout /etc/squid/unused.key -out /etc/squid/unused.pem \
    && chmod 640 /etc/squid/unused.key && chown root:proxy /etc/squid/unused.key \
    && mkdir -p /run/squid && chown proxy:proxy /run/squid
COPY squid.conf service-hosts.txt view-hosts.txt /etc/squid/
COPY sni_acl.py /opt/proxy/sni_acl.py
VOLUME ["/tmp"]
USER proxy
EXPOSE 3128 3129
ENTRYPOINT ["/usr/sbin/squid", "-N", "-f", "/etc/squid/squid.conf"]
