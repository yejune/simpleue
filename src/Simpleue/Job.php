<?php
/**
 * User: Javier Bravo
 * Date: 10/05/15
 */
namespace Simpleue;

interface Job
{
    public function manage($job);
    public function isStopJob($job);
}
