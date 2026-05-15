FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    build-essential cmake git libusb-1.0-0-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN git clone https://github.com/steve-m/librtlsdr.git && \
    cd librtlsdr && mkdir build && cd build && \
    cmake ../ -DDETACH_KERNEL_DRIVER=ON && \
    make -j$(nproc) && make install && ldconfig

ENV LD_LIBRARY_PATH=/usr/local/lib

EXPOSE 4321

CMD ["rtl_tcp", "-a", "0.0.0.0", "-p", "4321", "-n", "50"]