<?php declare(strict_types=1);

use PHPUnit\Framework\TestCase;

final class ClientTest extends TestCase
{
	/**
	 * Test Get and Set command.
	 * 	
	 * @return void
	 */
	public function testGetSet(): void
	{
	    $c = $this->getConn();
//	    $c->set('tests/1/',  '1');
	    $c->set('tests/demo/1',  '1');
		$this->assertSame('1', $c->get('tests/1'), __METHOD__);
	}

    public function testListDirs()
    {
        $client = $this->getConn();
        $dirs = $client->listDir('/');
        $this->assertCount(2, $dirs);
//        dd($dirs);
	}

    /**
     * @return \iCraftLtd\Component\Etcd\Client
     */
	protected function getConn(): \iCraftLtd\Component\Etcd\Client
    {
        return new \iCraftLtd\Component\Etcd\Client('http://192.168.0.218:2379');
    }
}