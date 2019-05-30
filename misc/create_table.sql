CREATE TABLE "public"."XXX_info" (
  "id" int4 NOT NULL DEFAULT nextval('ent_id_seq'::regclass),
  "ent_name" varchar(255),
  "ent_label" varchar(255),
  "ent_industry" varchar(255),
  "lon" float8,
  "lat" float8,
  PRIMARY KEY ("id")
)
;