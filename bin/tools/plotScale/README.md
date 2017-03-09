AWS Configuration

* Model: t2.medium

* OS

        $ cat /etc/*release
        DISTRIB_ID=Ubuntu
        DISTRIB_RELEASE=16.04
        DISTRIB_CODENAME=xenial
        DISTRIB_DESCRIPTION="Ubuntu 16.04.2 LTS"
        NAME="Ubuntu"
        VERSION="16.04.2 LTS (Xenial Xerus)"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu 16.04.2 LTS"
        VERSION_ID="16.04"
        HOME_URL="http://www.ubuntu.com/"
        SUPPORT_URL="http://help.ubuntu.com/"
        BUG_REPORT_URL="http://bugs.launchpad.net/ubuntu/"
        VERSION_CODENAME=xenial
        UBUNTU_CODENAME=xenial


        ubuntu@ip-172-31-7-0:~$ uname -a
        Linux ip-172-31-7-0 4.4.0-64-generic #85-Ubuntu SMP Mon Feb 20 11:50:30 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux

* Network: 1.00 Gbits/sec

    Server

        ubuntu@ip-172-31-1-132:~$ iperf -s
        ------------------------------------------------------------
        Server listening on TCP port 5001
        TCP window size: 85.3 KByte (default)
        ------------------------------------------------------------
        [  4] local 172.31.1.132 port 5001 connected with 172.31.7.0 port 33140
        [ ID] Interval       Transfer     Bandwidth
        [  4]  0.0-10.0 sec  1.17 GBytes  1.00 Gbits/sec

    Client

        ubuntu@ip-172-31-7-0:~$ iperf -c 172.31.1.132
        ------------------------------------------------------------
        Client connecting to 172.31.1.132, TCP port 5001
        TCP window size:  325 KByte (default)
        ------------------------------------------------------------
        [  3] local 172.31.7.0 port 33140 connected with 172.31.1.132 port 5001
        [ ID] Interval       Transfer     Bandwidth
        [  3]  0.0-10.0 sec  1.17 GBytes  1.00 Gbits/sec

* Disk

        ubuntu@ip-172-31-7-0:~$ sudo hdparm -Tt /dev/xvda1

        /dev/xvda1:
         Timing cached reads:   19672 MB in  2.00 seconds = 9847.62 MB/sec
         Timing buffered disk reads: 174 MB in  7.97 seconds =  21.82 MB/sec

        ubuntu@ip-172-31-7-0:~$ df -lh
        Filesystem      Size  Used Avail Use% Mounted on
        udev            2.0G     0  2.0G   0% /dev
        tmpfs           396M  5.7M  390M   2% /run
        /dev/xvda1      126G  4.1G  117G   4% /
        tmpfs           2.0G     0  2.0G   0% /dev/shm
        tmpfs           5.0M     0  5.0M   0% /run/lock
        tmpfs           2.0G     0  2.0G   0% /sys/fs/cgroup
        tmpfs           396M     0  396M   0% /run/user/1000

* Memory

        ubuntu@ip-172-31-7-0:~$ free -m
                      total        used        free      shared  buff/cache   available
        Mem:           3950          72        3560           5         317        3801
        Swap:             0           0           0

* CPU

        processor       : 0
        vendor_id       : GenuineIntel
        cpu family      : 6
        model           : 63
        model name      : Intel(R) Xeon(R) CPU E5-2676 v3 @ 2.40GHz
        stepping        : 2
        microcode       : 0x25
        cpu MHz         : 2394.480
        cache size      : 30720 KB
        physical id     : 0
        siblings        : 2
        core id         : 0
        cpu cores       : 2
        apicid          : 0
        initial apicid  : 0
        fpu             : yes
        fpu_exception   : yes
        cpuid level     : 13
        wp              : yes
        flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx rdtscp lm constant_tsc rep_good nopl xtopology eagerfpu pni pclmulqdq ss
        se3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm fsgsbase bmi1 avx2 smep bmi2 erms invpcid xsaveopt
        bugs            :
        bogomips        : 4788.96
        clflush size    : 64
        cache_alignment : 64
        address sizes   : 46 bits physical, 48 bits virtual
        power management:

        processor       : 1
        vendor_id       : GenuineIntel
        cpu family      : 6
        model           : 63
        model name      : Intel(R) Xeon(R) CPU E5-2676 v3 @ 2.40GHz
        stepping        : 2
        microcode       : 0x25
        cpu MHz         : 2394.480
        cache size      : 30720 KB
        physical id     : 0
        siblings        : 2
        core id         : 1
        cpu cores       : 2
        apicid          : 2
        initial apicid  : 2
        fpu             : yes
        fpu_exception   : yes
        cpuid level     : 13
        wp              : yes
        flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx rdtscp lm constant_tsc rep_good nopl xtopology eagerfpu pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm fsgsbase bmi1 avx2 smep bmi2 erms invpcid xsaveopt
        bugs            :
        bogomips        : 4788.96
        clflush size    : 64
        cache_alignment : 64
        address sizes   : 46 bits physical, 48 bits virtual
        power management: