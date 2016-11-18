#!perl -wT

use strictures 2;
use Test::More 1.001;
use Test::Exception 0.40;
use Test::Mock::Redis 0.18;
use JSON qw(decode_json);

use Log::Radis;

my $redis = Test::Mock::Redis->new;
my $radis = Log::Radis->new(redis => $redis);

sub lastmsg {
    decode_json($redis->lpop($radis->queue)//return);
}

isa_ok $radis->redis => 'Test::Mock::Redis';

throws_ok {
    $radis->log
} qr{log message without level}i;

throws_ok {
    $radis->log(0)
} qr{log message without level}i;

throws_ok {
    $radis->log(1)
} qr{log message without message}i;

my $msg = lastmsg();

is $msg => undef;

subtest test1 => sub {
    lives_ok {
        $radis->log(info => 'test1');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test1',
        level => 7,
    };
};

subtest test2 => sub {
    lives_ok {
        $radis->log(info => 'test2', foo => 'bar');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test2',
        level => 7,
        _foo => 'bar',
    };
};

subtest test3a => sub {
    lives_ok {
        $radis->log(info => 'test3a', host => 'foobar');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => 'foobar',
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test3a',
        level => 7,
    };
};

subtest test3b => sub {
    lives_ok {
        $radis->log(info => 'test3b', hostname => 'foobar');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => 'foobar',
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test3b',
        level => 7,
    };
};

subtest test4a => sub {
    lives_ok {
        $radis->log(info => 'test4a', time => 0);
    };

    $msg = lastmsg();

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        timestamp => 0,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test4a',
        level => 7,
    };
};

subtest test4b => sub {
    lives_ok {
        $radis->log(info => 'test4b', timestamp => 1);
    };

    $msg = lastmsg();

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        timestamp => 1,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test4b',
        level => 7,
    };
};

subtest test4c => sub {
    lives_ok {
        $radis->log(info => 'test4c', full_message => 'foobar');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test4c',
        full_message => 'foobar',
        level => 7,
    };
};

subtest test4d => sub {
    lives_ok {
        $radis->log(info => " \r \t \n short \n \n full \r \t \n ");
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        version => $Log::Radis::GELF_SPEC_VERSION,
        short_message => 'short',
        full_message => 'full',
        level => 7,
    };
};

subtest test5 => sub {
    lives_ok {
        $radis->log(fatal     => 1);
        $radis->log(emerg     => 1);
        $radis->log(emergency => 1);

        $radis->log(alert     => 2);

        $radis->log(crit      => 2);
        $radis->log(critical  => 3);

        $radis->log(error     => 4);
        $radis->log(err       => 4);

        $radis->log(warn      => 5);
        $radis->log(warning   => 5);

        $radis->log(note      => 6);
        $radis->log(notice    => 6);

        $radis->log(info      => 7);

        $radis->log(debug     => 8);

        $radis->log(trace     => 9);
        $radis->log(core      => 9);
    };

    while (my $msg = lastmsg()) {
        is $msg->{level} => $msg->{message};
    }
};

subtest test6a => sub {
    lives_ok {
        $radis->log(xxx => 'test6a');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test6a',
        level => undef,
    };
};

subtest test6b => sub {
    lives_ok {
        $radis->default_level('note');
        $radis->log(0 => 'test6b');
    };

    $msg = lastmsg();

    like delete($msg->{timestamp}) => qr{^\d+(\.\d+)?$};

    is_deeply $msg => {
        host => $Log::Radis::HOSTNAME,
        version => $Log::Radis::GELF_SPEC_VERSION,
        message => 'test6b',
        level => 6,
    };
};

subtest test7 => sub {
    lives_ok {
        $radis->push('foobar');
        is scalar($redis->lpop($radis->queue)) => 'foobar';
    };
};

$msg = lastmsg();

is $msg => undef;

done_testing;

