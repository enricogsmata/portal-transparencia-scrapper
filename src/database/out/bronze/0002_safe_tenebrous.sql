CREATE TABLE `file_download_history` (
	`id` serial AUTO_INCREMENT NOT NULL,
	`file_name` varchar(255) NOT NULL,
	`module_name` varchar(255) NOT NULL,
	`frequency` varchar(100) NOT NULL,
	`downloaded_at` timestamp NOT NULL,
	CONSTRAINT `file_download_history_id` PRIMARY KEY(`id`),
	CONSTRAINT `file_download_history_file_name_unique` UNIQUE(`file_name`)
);
