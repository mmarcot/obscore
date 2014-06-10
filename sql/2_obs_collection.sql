update obscore
set obs_collection = replace(replace((select collection
		from saada_loaded_file
		where obscore.oidsaada = saada_loaded_file.oidsaada),
		'_', '/'), 'AA', 'A+A');

