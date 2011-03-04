package Net::XIPCloud;

use strict;
use Fcntl;
use Data::Dumper;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use File::stat;
use LWP::UserAgent;
use HTTP::Request;
use IO::Socket::SSL;
require Exporter;

our $VERSION = '0.5';

@ISA = qw(Exporter);
@EXPORT = qw();

=head1 NAME

Net::XIPCloud - Perl extension for interacting with Internap's XIPCloud storage platform

=head1 SYNOPSIS

use Net::XIPCloud;

my $xip = Net::XIPCloud->new( username => 'myusername', password => 'mypassword );

$xip->connect();

$xip->cp("fromcontainer","fromobject","tocontainer","toobject");

$xip->mv("fromcontainer","fromobject","tocontainer","toobject");

$xip->file("somecontainer","someobject");

$xip->ls();

$xip->ls("mycontainer");

$xip->mkdir("newcontainer");

$xip->rmdir("somecontainer");

$xip->du();

$xip->du("somecontainer");

my $data = $xip->get_value("somecontainer","someobject");

$xip->get_file("somecontainer","someobject","/tmp/someobject");

$xip->put_value("somecontainer","someobject",$data,"text/html");

$xip->put_file("somecontainer","someobject","/tmp/someobject","text/html");

$xip->get_fhstream("somecontainer","someobject",*STDOUT);

$xip->rm("somecontainer","someobject");

=head1 DESCRIPTION

This perl module creates an XIPCloud object, which allows direct manipulation of objects and containers
within Internap's XIPCloud storage.

A valid XIPCloud account is required to use this module

=cut

=head2 new( username => 'username', password => 'password');

Returns a reference to a new XIPCloud object. This method requires passing of a valid username and password.

=cut

sub new() {
  my $class = shift;
  my %args = @_;
  my $self = {};

  bless $self, $class;

  $self->{api_url} = 'https://auth.storage.santa-clara.internapcloud.net:443/';
  $self->{api_version} = 'v1.0';

  foreach my $el (keys %args) {
    $self->{$el} = $args{$el};
  }
  return $self;
}

=head2 connect()

Connects to XIPCloud using the username and password provids in the new() call.

Method returns 1 for success and undef for failure.

=cut

sub connect() {
  my $self = shift;
  my $status = undef;

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(GET => $self->{api_url}.$self->{api_version});
  $req->header( 'X-AUTH-USER' => $self->{username} );
  $req->header( 'X-AUTH-KEY' => $self->{password} );

  my $res = $ua->request($req);

  if ($res->is_success) {
    $status = 1;
    $self->{connected} = 1;
    $self->{storage_token} = $res->header( 'x-storage-token' );
    $self->{storage_url} = $res->header( 'x-storage-url' );

    $self->{debug} && print "connected: token [".$self->{storage_token}."] url [".$self->{storage_url}."]\n";
  }
  else {
    $self->{debug} && print "connection failed\n";
  }

  return $status;
}

=head2 ls([CONTAINER])

Depending on the calling arguments, this method returns the list of containers or list
of objects within a single container as an array reference.

Limit and marker values may be provided (see API documentation) for pagination.

=cut

sub ls() {
  my $self = shift;
  my $container = shift;
  my $limit = shift;
  my $marker = shift;
  my $list = [];

  return undef unless ($self->{connected});

  my $ua = LWP::UserAgent->new;
  my $requrl = $self->{storage_url};
  if ($container) {
    $requrl.='/'.$container;
  }
  if ($limit || $marker) {
    $requrl.="?limit=$limit&marker=$marker";
  }
  my $req = HTTP::Request->new(GET => $requrl);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );

  my $res = $ua->request($req);

  if ($res->is_success) {
    my @raw = split("\n",$res->content);
    foreach (@raw) {
      next if /^$/;
      push @$list, $_;
    }

    $self->{debug} && print "ls: success - got [".scalar(@$list)."] elements\n";
  }
  else {
    undef $list;
    $self->{debug} && print "ls: failed\n";
  }

  return $list;
}

=head2 file("somecontainer","someobject")

This call returns metadata about a specific object.

=cut

sub file() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $container && $object);

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(HEAD => $self->{storage_url}.'/'.$container.'/'.$object);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );

  my $res = $ua->request($req);

  if ($res->is_success) {
    $status->{size} = $res->header("content-length");
    $status->{mtime} = $res->header("last-modified");
    $status->{md5sum} = $res->header("etag");
    $status->{type} = $res->header("content-type");

    $self->{debug} && print "file: success [$container/$object]\n";
  }
  else {
    $self->{debug} && print "file: failed [$container/$object]\n";
  }

  return $status;
}

=head2 cp("fromcontainer","fromobject",'tocontainer","toobject");

Copy the contents of one object to another

=cut

sub cp() {
  my $self = shift;
  my $scontainer = shift;
  my $sobject = shift;
  my $dcontainer = shift;
  my $dobject = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $scontainer && $sobject && $dcontainer && $dobject);

  my $src = $self->file($scontainer,$sobject);
  return undef unless (ref $src eq 'HASH');
  my $type = $src->{type};

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(COPY => $self->{storage_url}.'/'.$scontainer.'/'.$sobject);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Destination' => $dcontainer.'/'.$dobject);
  $req->header( 'Content-type' => $type);
  my $res = $ua->request($req);

  if ($res->is_success) {
    $status = 1;
    $self->{debug} && print "cp: success [$scontainer/$sobject]=>[$dcontainer/$dobject]\n";
  }
  else {
    $self->{debug} && print "cp: failed [$scontainer/$sobject]=>[$dcontainer/$dobject]\n";
  }
  return $status;
}

=head2 mv("fromcontainer","fromobject",'tocontainer","toobject");

Rename an object, clobbering any existing object

=cut

sub mv() {
  my $self = shift;
  my $scontainer = shift;
  my $sobject = shift;
  my $dcontainer = shift;
  my $dobject = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $scontainer && $sobject && $dcontainer && $dobject);
  return if ( ($scontainer eq $dcontainer) && ($sobject eq $dobject));

  my $src = $self->file($scontainer,$sobject);
  return undef unless (ref $src eq 'HASH');
  my $type = $src->{type};

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(COPY => $self->{storage_url}.'/'.$scontainer.'/'.$sobject);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Destination' => $dcontainer.'/'.$dobject);
  $req->header( 'Content-type' => $type);
  my $res = $ua->request($req);

  if ($res->is_success) {

    if ( $self->rm($scontainer,$sobject) ) {
      $status = 1;
      $self->{debug} && print "mv: success [$scontainer/$sobject]=>[$dcontainer/$dobject]\n";
    }
    else {
      $self->{debug} && print "mv: failed [$scontainer/$sobject]=>[$dcontainer/$dobject]\n";
    }
  }
  else {
    $self->{debug} && print "mv: failed [$scontainer/$sobject]=>[$dcontainer/$dobject]\n";
  }
  return $status;
}

=head2 mkdir("somecontainer")

This method creates a new container. It returns 1 for success and undef for failure.

=cut

sub mkdir() {
  my $self = shift;
  my $container = shift;
  my $status = undef;

  return undef unless ($self->{connected});
  return undef unless $container;

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(PUT => $self->{storage_url}.'/'.$container);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Content-Length' => '0' );
  my $res = $ua->request($req);

  if ($res->is_success) {
    $status = 1;

    $self->{debug} && print "mkdir: success [$container]\n";
  }
  else {
    $self->{debug} && print "mkdir: failed [$container]\n";
  }

  return $status;
}

=head2 rmdir("somecontainer")

This method removes a container and its contents. It returns 1 for success and undef for failure.

=cut

sub rmdir() {
  my $self = shift;
  my $container = shift;
  my $status = undef;

  return undef unless ($self->{connected});
  return undef unless $container;

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(DELETE => $self->{storage_url}.'/'.$container);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Content-Length' => '0' );
  my $res = $ua->request($req);

  if ($res->is_success) {
    $status = 1;
 
    $self->{debug} && print "rmdir: success [$container]\n";   
  }
  else {
    $self->{debug} && print "rmdir: failed [$container]\n";
  }
  return $status;
}

=head2 du([CONTAINER])

Depending on calling arguments, this method returns account or container-level statistics as 
a hash reference.

=cut

sub du() {
  my $self = shift;
  my $container = shift;
  my $status = undef;

  return undef unless ($self->{connected});

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(HEAD => $self->{storage_url}.($container?'/'.$container:''));
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  my $res = $ua->request($req);

  if ($res->is_success) {
    if ($container) {
      $status->{bytes} = $res->header('x-container-bytes-used');
      $status->{objects} = $res->header('x-container-object-count');
    }
    else {
      $status->{bytes} = $res->header('x-account-bytes-used');
      $status->{objects} = $res->header('x-account-object-count');
      $status->{containers} = $res->header('x-account-container-count');
    }

    $self->{debug} && print "du: success\n";
  }
  else{
    $self->{debug} && print "du: failed\n";
  }
  return $status;
}

=head2 get_value("somecontainer","someobject")

This method returns a scalar value, containing the body of the requested object.

=cut

sub get_value() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  my $data = undef;

  return undef unless ($self->{connected} && $container && $object);

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(GET => $self->{storage_url}.'/'.$container.'/'.$object);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );

  my $res = $ua->request($req);

  if ($res->is_success) {
    $data = $res->content;

    $self->{debug} && print "get_value: success for [$container/$object]\n";
  }
  else {
    $self->{debug} && print "get_value: failed for [$container/$object]\n";
  }
  return $data;
}

=head2 put_value("somecontainer","someobject","..data..","text/html")

This method places the contents of a passed scalar into the specified container and object.

Content-type may be specified, but is optional. It defaults to "text/plain"

=cut

sub put_value() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  my $data = shift;
  my $content_type = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $container && $object && $data);

  unless ($content_type) {
    $content_type = 'text/plain';
  }

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(PUT => $self->{storage_url}.'/'.$container.'/'.$object);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Content-type' => $content_type);
  $req->content( $data );  

  my $res = $ua->request($req);

  if ($res->is_success) {
    $data = $res->content;

    $self->{debug} && print "put_value: success for [$container/$object]\n";
  }
  else {
    $self->{debug} && print "put_value: failed for [$container/$object]\n";
  }
  return $status;
}

=head2 get_file("somecontainer","someobject","/tmp/destfile")

This method places the contents of the requested object in a target location of the filesystem.

=cut

sub get_file() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  my $tmpfile = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $container && $object && $tmpfile);

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(GET => $self->{storage_url}.'/'.$container.'/'.$object);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );

  my $res = $ua->request($req,$tmpfile);

  if ($res->is_success) {
    $status = 1;

    $self->{debug} && print "get_file: success for [$container/$object]\n";
  }
  else {
    $self->{debug} && print "get_file: failed for [$container/$object]\n";
  }
  return $status;
}

=head2 put_file("somecontainer","someobject","/tmp/sourcefile","text/html")

This method places the contents of a specified source file into an object.

=cut

sub put_file() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  my $srcfile = shift;
  my $content_type = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $container && $object && (-e $srcfile) );

  unless ($content_type) {
    $content_type = 'text/plain';
  }

  my $size = stat($srcfile)->size;
  open(IN, $srcfile);
  binmode IN;

  my $reader = sub { 
    read IN, my $buf, 65536;
    return $buf;
  };

  $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD = 1;
  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(PUT => $self->{storage_url}.'/'.$container.'/'.$object);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Content-type' => $content_type);
  $req->header( 'Content-length' => $size );
  $req->content($reader);
  my $res = $ua->request($req);

  if ($res->is_success) {
    $status = 1;

    $self->{debug} && print "put_file: success for [$container/$object]\n";
  }
  else {
    $self->{debug} && print "put_file: failed for [$container/$object]\n";
  }
  return $status;
}

=head2 get_fhstream("somecontainer","someobject",*FILE)

This method takes a container, object and open file handle as arguments.
It retrieves the object in chunks, which it writes to *FILE as they are received.

=cut

sub get_fhstream() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  local (*OUT) = shift;
  my $status = undef;

  return undef unless ($self->{connected} && $container && $object && *OUT);
  return undef unless ( (O_WRONLY | O_RDWR) & fcntl (OUT, F_GETFL, my $slush));

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(GET => $self->{storage_url}.'/'.$container.'/'.$object);

  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  my $res = $ua->request($req,
    sub {
      my ($chunk,$res) = @_;
      print OUT $chunk;
    }
  );

  if ($res->is_success) {
    $status = 1;

    $self->{debug} && print "get_fhstream: success for [$container/$object]\n";
  }
  else {
    $self->{debug} && print "get_fhstream: failed for [$container/$object]\n";
  }
  return $status;
}

=head2 rm("somecontainer","someobject")

This method removes an object.

=cut

sub rm() {
  my $self = shift;
  my $container = shift;
  my $object = shift;
  my $status = undef;

  return undef unless ($self->{connected});
  return undef unless $container && $object;

  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(DELETE => $self->{storage_url}.'/'.$container.'/'.$object);
  $req->header( 'X-STORAGE-TOKEN' => $self->{storage_token} );
  $req->header( 'Content-Length' => '0' );
  my $res = $ua->request($req);

  if ($res->is_success) {
    $status = 1;

    $self->{debug} && print "rm: success for [$container/$object]\n";
  }
  else {
    $self->{debug} && print "rm: failed for [$container/$object]\n";
  }
  return $status;
}

1;
__END__

=head1 AUTHOR

Dennis Opacki, dopacki@internap.com

=head1 SEE ALSO

perl(1).

=cut
