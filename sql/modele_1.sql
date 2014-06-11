insert into obscore(oidsaada, sky_pixel_csa, s_ra, s_dec, access_url, s_region, s_fov)
	select oidsaada, sky_pixel_csa, pos_ra_csa, pos_dec_csa, product_url_csa, shape_csa, (size_alpha_csa+size_delta_csa)/2 
	from {table};
