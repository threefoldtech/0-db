all release:
	$(MAKE) -C libzdb $@
	$(MAKE) -C zdbd $@
	$(MAKE) -C tools $@

	cp -f zdbd/zdb bin/
	cp -f tools/integrity-check/integrity-check bin/zdb-integrity-check
	cp -f tools/index-dump/index-dump bin/zdb-index-dump
	# cp -f tools/compaction/compaction bin/zdb-compaction
	cp -f tools/namespace-editor/namespace-editor bin/zdb-namespace-editor

clean:
	$(MAKE) -C libzdb $@
	$(MAKE) -C zdbd $@
	$(MAKE) -C tools $@

mrproper:
	$(MAKE) -C libzdb $@
	$(MAKE) -C zdbd $@
	$(MAKE) -C tools $@
	$(RM) bin/*
