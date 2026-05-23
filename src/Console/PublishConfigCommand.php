<?php

declare(strict_types=1);

namespace Bow\Microservice\Console;

use Bow\Console\AbstractCommand;

class PublishConfigCommand extends AbstractCommand
{
    /**
     * Copy the package's default config/microservice.php into the host
     * application's config directory.
     *
     * Refuses to overwrite an existing file unless `--force` is passed, so a
     * user who has already customized their config can't lose changes by
     * re-running the command.
     */
    public function run(): void
    {
        $source = dirname(__DIR__, 2) . '/config/microservice.php';
        $destination = $this->setting->getConfigDirectory() . '/microservice.php';

        if (!is_file($source)) {
            echo "\033[0;31mPackage config file is missing: {$source}\033[00m\n";

            exit(1);
        }

        if (is_file($destination) && !$this->arg->getParameter('--force')) {
            echo "\033[0;31mconfig/microservice.php already exists. Use --force to overwrite.\033[00m\n";

            exit(1);
        }

        if (!is_dir(dirname($destination))) {
            mkdir(dirname($destination), 0o755, true);
        }

        if (!copy($source, $destination)) {
            echo "\033[0;31mFailed to copy config to {$destination}.\033[00m\n";

            exit(1);
        }

        echo "\033[0;32mPublished config/microservice.php\033[00m\n";

        exit(0);
    }
}
