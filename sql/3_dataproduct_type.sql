update obscore
set dataproduct_type = 'IMAGE'
where obs_collection in (select name_class
	from saada_metaclass_image
	where name_origin LIKE 'NAXIS_'
	group by name_class
	having count(*) = 2);

update obscore
set dataproduct_type = 'CUBE'
where obs_collection in (select name_class
	from saada_metaclass_image
	where name_origin LIKE 'NAXIS_'
	group by name_class
	having count(*) > 2);
