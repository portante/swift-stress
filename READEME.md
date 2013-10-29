# Test

	$ tar -xJf test-lists-anon-4.8mil.tar.xz
	$ mkdir log run
	$ # Edit "cmd" file to set the proper run parameters, container name, etc.
	$ # Typically one can use the container name to encode the test parameters
	$ # to make it easier to track look at URLs in /var/log/messages. Note that
	$ # the "cmd" script also creates the container first before running the
	$ # uploader.
	$ nohup ./cmd >> log/cmd.log 2>&1 &
	$ tail -F log/cmd.log
	$ # Results land in log/*-results.log
