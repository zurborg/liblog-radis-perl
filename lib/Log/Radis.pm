use strictures 2;

package Log::Radis;

# ABSTRACT: Radis is a graylog logging radio through a redis database

use Moo 2;
use Redis 1.980;
use JSON 2.90 qw(encode_json);
use Time::HiRes 1.9726;
use Sys::Hostname ;
use Carp qw(croak carp);
use Types::Standard 1 qw(HasMethods);

our $GELF_SPEC_VERSION = '1.1';
our $HOSTNAME = hostname();

# VERSION

=head1 SYNOPSIS

    # default values shown below
    my $radis = Log::Radis->new(
        server => 'localhost:6379',
        reconnect => 5,
        every => 1,
        queue => 'graylog-radis:queue',
    );

    $radis->log(error => "This is a non-urgent error");

=head1 DESCRIPTION

Radis (from I<Radio> and I<Redis>) is a concept of caching GELF messages in a Redis DB. Redis provides a I<reliable queue> via the I<(B)RPOPLPUSH> command. See L<http://redis.io/commands/rpoplpush> for more information about that mechanism.

The implementation of a Radis client is quite simple: just push a GELF message with the L<LPUSH> command onto the queue. A collector fetches the messages from the queue and inserts them into a Graylog2 server, for example.

=cut

=attr server

The Redis DB server we should connect to.

See L<Redis/server> for allowed values. Defaults to C<localhost:6379>.

=cut

has server => (
    is => 'ro',
    default => 'localhost:6379',
);

=attr reconnect

Re-try connecting to the Redis DB up to I<reconnect> seconds. C<0> disables auto-reconnect.

See L<Redis/reconnect> for more information.

=cut

has reconnect => (
    is => 'ro',
    default => 5,
);

=attr every

Re-try connection to the Redis DB every I<every> milliseconds.

See L<Redis/every> for more information.

=cut

has every => (
    is => 'ro',
    default => 1,
);

=attr queue

The name of the list, which gelf streams are pushed to. Defaults to C<graylog-radis:queue>.

=cut

has queue => (
    is => 'ro',
    default => 'graylog-radis:queue',
);

=attr redis

Set or get the Redis instance. Defaults to an instance of L<Redis>. Any blessed object which implements the method C<lpush> is allowed.

For your own Redis implementation use this attribute in the constructor. That should work:

    my $redis = AnyEvent::Redis->new(...);
    my $radis = Log::Radis->new(redis => $redis);
    my $cv = $radis->log(...);
    $cv->recv;

=cut

has redis => (
    is => 'lazy',
    isa => HasMethods[qw[ lpush ]],
    builder => sub {
        my $self = shift;
        return Radis->new(
            server    => $self->server,
            reconnect => $self->reconnect,
            every     => $self->every,
        );
    }
);

=head1 LOG LEVELS

These levels are known and supposed to be compatible to various other logging engines.

    Identifier | Numeric level
    -----------+--------------
    fatal      | 1
    emerg      | 1
    emergency  | 1
    -----------+---
    alert      | 2
    -----------+---
    crit       | 3
    critical   | 3
    -----------+---
    error      | 4
    err        | 4
    -----------+---
    warn       | 5
    warning    | 5
    -----------+---
    note       | 6
    notice     | 6
    -----------+---
    info       | 7
    -----------+---
    debug      | 8
    -----------+---
    trace      | 9
    core       | 9

=cut

my %levels = (
    fatal     => 1,
    emerg     => 1,
    emergency => 1,

    alert     => 2,

    crit      => 2,
    critical  => 3,

    error     => 4,
    err       => 4,

    warn      => 5,
    warning   => 5,

    note      => 6,
    notice    => 6,

    info      => 7,

    debug     => 8,

    trace     => 9,
    core      => 9,
);

=method log

    $radis->log($level, $message, %additional_gelf_params);
    $radis->log(alert => "This is an alert!");
    $radis->log(notice => "Look at this.", additional_param => $additional_value);

Additional GELF params must be prefixed with an underscore - but this method does that for you.

Overrides are only allowed for I<host>/I<hostname> and I<timestamp>/I<time> params. They defaults to the system hostname and the current timestamp from L<Time::HiRes/time>.

=cut

sub log {
    my $self = shift;
    my ($level, $message, %gelf) = @_;

    croak "log message without level" unless defined $level;
    croak "log message without message" unless defined $message;

    # replace level with numeric code, if needed
    $level = $levels{lc($level)} unless $level =~ m{^\d$};

    # addiotional field are only allowed with a prefixed underscore
    # and strip off all unallowed chars
    %gelf = map {
        m{^_[\w\.\-]+$}i
    ?
        (
            lc($_)
        ,
            $gelf{$_}
        )
    :
        (
            '_'.s{[^\w\.\-]+}{}gr
        ,
            $gelf{$_}
        )
    } grep { defined $gelf{$_} } keys %gelf;

    # graylog omit the id field automatically
    if (exists $gelf{_id}) {
        carp "log message with id is not allowed";
        delete $gelf{_id};
    }

    # preserve params, which are allowed by client
    # including some mispelled ones
    $gelf{host}      = delete $gelf{_hostname}  if defined $gelf{_hostname};
    $gelf{host}      = delete $gelf{_host}      if defined $gelf{_host};
    $gelf{timestamp} = delete $gelf{_time}      if defined $gelf{_time};
    $gelf{timestamp} = delete $gelf{_timestamp} if defined $gelf{_timestamp};

    # hostname defaults to system hostname...
    $gelf{host} //= $HOSTNAME;

    # ...and timestamp with milliseconds by default
    $gelf{timestamp} //= Time::HiRes::time();

    # graylog seems to have problems with float values in json
    # so force string, which works fine
    $gelf{timestamp} = ''.$gelf{timestamp};

    $gelf{short_message} = $message;
    $gelf{version} = $GELF_SPEC_VERSION;
    $gelf{level} = $level;

    $self->push(\%gelf);
}

=method push

    $radis->push({ ... });

    Raw-push a gelf message onto queue. If the argument is not a HashRef, it will be encoded to a JSON string.

    The input is not validated, so be careful what you push onto the queue.

=cut

sub push {
    my ($self, $gelf) = @_;
    if (ref $gelf eq 'HASH') {
        $gelf = encode_json($gelf);
    }
    $self->redis->lpush($self->queue, $gelf);
}

1;
