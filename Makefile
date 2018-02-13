all release:
	$(MAKE) -C src $@
	$(MAKE) -C tools $@

	cp -f src/zdb bin/
	cp -f tools/integrity-check bin/zdb-integrity-check

clean mrproper:
	$(MAKE) -C src $@
	$(MAKE) -C tools $@
	$(RM) bin/*
