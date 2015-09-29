package Catmandu::Store::MongoDB;

use Catmandu::Sane;
use Moo;
use Catmandu::Store::MongoDB::Bag;
use MongoDB;

with 'Catmandu::Store';

our $VERSION = '0.0401';

=head1 NAME

Catmandu::Store::MongoDB - A searchable store backed by MongoDB

=head1 SYNOPSIS

    # On the command line
    $ catmandu import -v JSON --multiline 1 to MongoDB --database_name bibliography --bag books < books.json
    $ catmandu export MongoDB --database_name bibliography --bag books to YAML
    $ catmandu count MongoDB --database_name bibliography --bag books --query '{"PublicationYear": "1937"}'

    # In perl
    use Catmandu::Store::MongoDB;

    my $store = Catmandu::Store::MongoDB->new(database_name => 'test');

    my $obj1 = $store->bag->add({ name => 'Patrick' });

    printf "obj1 stored as %s\n" , $obj1->{_id};

    # Force an id in the store
    my $obj2 = $store->bag->add({ _id => 'test123' , name => 'Nicolas' });

    my $obj3 = $store->bag->get('test123');

    $store->bag->delete('test123');

    $store->bag->delete_all;

    # All bags are iterators
    $store->bag->each(sub { ... });
    $store->bag->take(10)->each(sub { ... });

    # Search
    my $hits = $store->bag->search(query => '{"name":"Patrick"}');
    my $hits = $store->bag->search(query => '{"name":"Patrick"}' , sort => { age => -1} );
    my $hits = $store->bag->search(query => {name => "Patrick"} , start => 0 , limit => 100);
    
    my $next_page = $hits->next_page;
    my $hits = $store->bag->search(query => '{"name":"Patrick"}' , page => $next_page);

    my $iterator = $store->bag->searcher(query => {name => "Patrick"});

=head1 DESCRIPTION

A Catmandu::Store::MongoDB is a Perl package that can store data into
L<MongoDB> databases. The database as a whole is called a 'store'.
Databases also have compartments (e.g. tables) called Catmandu::Bag-s.

=head1 DEPRECATION NOTICE

The following connection parameters are depreacted and will be removed in a future version of this module:
    
    - connect_retry
    - connect_retry_sleep

=head1 METHODS

=head2 new(database_name => $name , %opts )

Create a new Catmandu::Store::MongoDB store with name $name. Optionally 
provide connection parameters (see L<MongoDB::MongoClient> for possible 
options).

=head2 bag($name)

Create or retieve a bag with name $name. Returns a L<Catmandu::Bag>.

=head2 client

Return the L<MongoDB::MongoClient> instance.

=head2 database

Return a L<MongoDB::Database> instance.

=cut

my $CLIENT_ARGS = [
    qw(
        connect_timeout_ms
        db_name
        dt_type
        find_master
        host
        j
        password
        socket_timeout_ms
        ssl
        username
        w
        wtimeout
        )
];

# deprecated. remove this attribute in a future version
has connect_retry => ( is => 'ro' );
# deprecated. remove this attribute in a future version
has connect_retry_sleep => ( is => 'ro' );
has client        => (is => 'ro', lazy => 1, builder => '_build_client');
has database_name => (is => 'ro', required => 1);
has database      => (is => 'ro', lazy => 1, builder => '_build_database');

sub _build_client {
    my $self = shift;
    my $args = delete $self->{_args};
    my $host = $self->{_args}->{host} // 'mongodb://localhost:27017';
    $self->log->debug("Build MongoClient for $host");
    my $client = MongoDB::MongoClient->new($args);
    return $client;
}

sub _build_database {
    my $self          = shift;
    my $database_name = $self->database_name;
    $self->log->debug("Build or get database $database_name");
    my $database = $self->client->get_database($database_name);
    return $database;
}

sub BUILD {
    my ($self, $args) = @_;

    if ( $self->{connect_retry} || $self->{connect_retry_sleep} ) {
        warnings::warnif( "deprecated",
            "Connection parameter \'connect_retry\' and \'connect_retry_sleep\' are deprecated and will be removed in future versions of Catmandu::Store::MongoDB"
        );
    }

    $self->{_args} = {};
    for my $key (@$CLIENT_ARGS) {
        $self->{_args}{$key} = $args->{$key} if exists $args->{$key};
    }
}

=head1 SEE ALSO

L<Catmandu::Bag>, L<Catmandu::Searchable> , L<MongoDB::MongoClient>

=head1 AUTHOR

Nicolas Steenlant, C<< <nicolas.steenlant at ugent.be> >>

=head1 CONTRIBUTORS

Johann Rolschewski, C<< <jorol at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
