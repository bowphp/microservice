<?php

declare(strict_types=1);

namespace Bow\Microservice\Console;

use Bow\Console\AbstractCommand;
use Bow\Console\Generator;

class GenerateConsumerCommand extends AbstractCommand
{
  /**
   * Add consumer
   *
   * @param  string $consumer
   * @return void
   */
  public function run(string $consumer): void
  {
    $generator = new Generator(
      $this->setting->getBaseDirectory() . "/app/Consumers",
      $consumer
    );

    $generator->setStubPath(__DIR__ . '/../../stubs/consumer.stub');

    if ($generator->fileExists()) {
      echo "\033[0;31mThe consumner already exists.\033[00m\n";

      exit(1);
    }

    $generator->writeFromDefineStubeFile([
      'baseNamespace' => $this->namespaces['consumer'] ?? 'App\\Consumers',
      'className' => $consumer,
    ]);

    echo "\033[0;32mThe consumer has been well created.\033[00m\n";

    exit(0);
  }
}
