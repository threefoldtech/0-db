all release:
	$(MAKE) -C src $@
	$(MAKE) -C tools $@

	cp -f src/zdb bin/
	cp -f tools/integrity-check/integrity-check bin/zdb-integrity-check
	cp -f tools/index-dump/index-dump bin/zdb-index-dump

clean:
	$(MAKE) -C src $@
	$(MAKE) -C tools $@

mrproper:
	$(MAKE) -C src $@
	$(MAKE) -C tools $@
	$(RM) bin/*
