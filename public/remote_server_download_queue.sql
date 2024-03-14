create table remote_server_download_queue
(
    download_id     bigserial
        constraint remote_server_download_queue_pk
            primary key,
    url             text                  not null,
    target_hostname text                  not null,
    requesting_user text,
    downloaded      boolean default false not null
);

alter table remote_server_download_queue
    owner to postgres;

