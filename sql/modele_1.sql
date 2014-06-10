insert into obscore(oidsaada, sky_pixel_csa, s_ra, s_dec)
	select oidsaada, sky_pixel_csa, pos_ra_csa, pos_dec_csa from {table};
