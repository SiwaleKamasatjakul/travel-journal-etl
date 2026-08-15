-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.accounts (
  user_id integer NOT NULL DEFAULT nextval('accounts_user_id_seq'::regclass),
  username character varying NOT NULL UNIQUE,
  email character varying NOT NULL UNIQUE,
  passwordhash character varying NOT NULL,
  preferences text,
  role character varying DEFAULT 'user'::character varying,
  created_at timestamp with time zone DEFAULT now(),
  last_login timestamp with time zone,
  flag boolean DEFAULT false,
  image_id integer,
  CONSTRAINT accounts_pkey PRIMARY KEY (user_id),
  CONSTRAINT accounts_image_id_fkey FOREIGN KEY (image_id) REFERENCES public.images(id)
);
CREATE TABLE public.follows (
  follower_id integer NOT NULL,
  following_id integer NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  CONSTRAINT follows_pkey PRIMARY KEY (follower_id, following_id),
  CONSTRAINT follows_follower_id_fkey FOREIGN KEY (follower_id) REFERENCES public.accounts(user_id),
  CONSTRAINT follows_following_id_fkey FOREIGN KEY (following_id) REFERENCES public.accounts(user_id)
);
CREATE TABLE public.trip_posts (
  id integer NOT NULL DEFAULT nextval('trip_posts_id_seq'::regclass),
  user_id integer NOT NULL,
  image_id integer,
  trip_name character varying NOT NULL,
  location_country_tag_id integer,
  activity_type_tag_id integer,
  total_distance integer,
  caption text,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  trip_rating numeric,
  CONSTRAINT trip_posts_pkey PRIMARY KEY (id),
  CONSTRAINT trip_posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id)
);
CREATE TABLE public.trip_stops (
  id integer NOT NULL DEFAULT nextval('trip_stops_id_seq'::regclass),
  trip_post_id integer NOT NULL,
  user_id integer NOT NULL,
  image_id integer,
  location_title character varying NOT NULL,
  google_maps_address_id integer,
  time timestamp with time zone NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  address text,
  category character varying,
  rating numeric,
  review_count integer,
  price_range character varying,
  status character varying,
  hours character varying,
  lat numeric,
  lng numeric,
  duration character varying,
  CONSTRAINT trip_stops_pkey PRIMARY KEY (id),
  CONSTRAINT trip_stops_trip_post_id_fkey FOREIGN KEY (trip_post_id) REFERENCES public.trip_posts(id),
  CONSTRAINT trip_stops_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id)
);
CREATE TABLE public.google_maps_address (
  id integer NOT NULL DEFAULT nextval('google_maps_address_id_seq'::regclass),
  name character varying NOT NULL,
  address character varying NOT NULL,
  latitude numeric NOT NULL,
  longitude numeric NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  city character varying,
  state_province character varying,
  country character varying,
  postal_code character varying,
  place_id character varying,
  street character varying,
  neighborhood character varying,
  district character varying,
  building character varying,
  CONSTRAINT google_maps_address_pkey PRIMARY KEY (id)
);
CREATE TABLE public.location_country_tag (
  id integer NOT NULL DEFAULT nextval('location_country_tag_id_seq'::regclass),
  country_code integer NOT NULL,
  trip_post_id integer NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  CONSTRAINT location_country_tag_pkey PRIMARY KEY (id),
  CONSTRAINT location_country_tag_trip_post_id_fkey FOREIGN KEY (trip_post_id) REFERENCES public.trip_posts(id)
);
CREATE TABLE public.activity_tag (
  id integer NOT NULL DEFAULT nextval('activity_tag_id_seq'::regclass),
  activity_code integer NOT NULL,
  trip_post_id integer NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  CONSTRAINT activity_tag_pkey PRIMARY KEY (id),
  CONSTRAINT activity_tag_trip_post_id_fkey FOREIGN KEY (trip_post_id) REFERENCES public.trip_posts(id)
);
CREATE TABLE public.activity_code (
  id integer NOT NULL DEFAULT nextval('activity_code_id_seq'::regclass),
  activity_code integer NOT NULL UNIQUE,
  activity_name character varying NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  emoji character varying,
  CONSTRAINT activity_code_pkey PRIMARY KEY (id)
);
CREATE TABLE public.country_code (
  id integer NOT NULL DEFAULT nextval('country_code_id_seq'::regclass),
  country_code integer NOT NULL UNIQUE,
  country_name character varying NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  CONSTRAINT country_code_pkey PRIMARY KEY (id)
);
CREATE TABLE public.bio (
  id integer NOT NULL DEFAULT nextval('bio_id_seq'::regclass),
  user_id integer NOT NULL,
  bio_text text NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  CONSTRAINT bio_pkey PRIMARY KEY (id),
  CONSTRAINT bio_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id)
);
CREATE TABLE public.plan_trip (
  id integer NOT NULL DEFAULT nextval('plan_trip_id_seq'::regclass),
  user_id integer NOT NULL,
  trip_name character varying NOT NULL,
  start_date date,
  end_date date,
  is_public boolean DEFAULT false,
  created_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  details text,
  CONSTRAINT plan_trip_pkey PRIMARY KEY (id),
  CONSTRAINT plan_trip_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id)
);
CREATE TABLE public.plan_trip_date (
  id integer NOT NULL DEFAULT nextval('plan_trip_date_id_seq'::regclass),
  plan_trip_id integer NOT NULL,
  date date,
  time time without time zone,
  google_map_address_id integer,
  flag boolean DEFAULT false,
  CONSTRAINT plan_trip_date_pkey PRIMARY KEY (id),
  CONSTRAINT plan_trip_date_plan_trip_id_fkey FOREIGN KEY (plan_trip_id) REFERENCES public.plan_trip(id),
  CONSTRAINT plan_trip_date_google_map_address_id_fkey FOREIGN KEY (google_map_address_id) REFERENCES public.google_maps_address(id)
);
CREATE TABLE public.images (
  id integer NOT NULL DEFAULT nextval('images_id_seq'::regclass),
  user_id integer NOT NULL,
  image_url character varying NOT NULL,
  upload_at timestamp with time zone DEFAULT now(),
  flag boolean DEFAULT false,
  CONSTRAINT images_pkey PRIMARY KEY (id),
  CONSTRAINT images_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id)
);
CREATE TABLE public.post_likes (
  id integer NOT NULL DEFAULT nextval('post_likes_id_seq'::regclass),
  user_id integer NOT NULL,
  post_id integer NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT post_likes_pkey PRIMARY KEY (id),
  CONSTRAINT post_likes_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id),
  CONSTRAINT post_likes_post_id_fkey FOREIGN KEY (post_id) REFERENCES public.trip_posts(id)
);
CREATE TABLE public.post_bookmarks (
  id integer NOT NULL DEFAULT nextval('post_bookmarks_id_seq'::regclass),
  user_id integer NOT NULL,
  post_id integer NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT post_bookmarks_pkey PRIMARY KEY (id),
  CONSTRAINT post_bookmarks_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts(user_id),
  CONSTRAINT post_bookmarks_post_id_fkey FOREIGN KEY (post_id) REFERENCES public.trip_posts(id)
);
