CREATE TABLE `logs` (
	`id` serial AUTO_INCREMENT,
	`category` varchar(255) DEFAULT '',
	`module` varchar(255) DEFAULT '',
	`context` varchar(255) DEFAULT '',
	`message` varchar(255) DEFAULT '',
	`data` text DEFAULT (''),
	`created_at` timestamp NOT NULL
);
