package RecoveryLogger;

use IO::File;
use File::Path;
use Data::Dumper;


$|=1;

our $filename;

sub import 
{
 my $self = shift;
 $filename = shift;
}


sub new
{
	my ( $self ,$dir , $file ) = @_;
	my $data = {};
	$file = $filename if ( ! defined $file ); 

    mkpath( $dir ) if( ! -e $dir);
    
    $data->{OUT_FILE_NAME} = "$dir/$file";
	$data->{RECOVERY} = 0;
	$data->{DATA} = ( -e "$dir/$file" ) ? _init( "$dir/$file" ) : {};
    
    $data->{OUT_FILE} = new IO::File("+>> $dir/$file");
    $data->{RECOVERY} = 1 if ( scalar keys %{$data->{DATA}}  );
     
    $data->{COMMIT_THRESHOLD} = 5;
    $data->{COMMIT_BACKLOG} = 0;
     
 #   print keys %{$data->{DATA}};
    
    #print "####################\n";  
    #print Dumper($data->{DATA});
    #print "#####################\n";
    
    $Data::Dumper::Terse = 1;       
    $Data::Dumper::Indent = 0;  
     
   
	
	bless $data, $self;
}

sub _init()
{
  open ( REC_FILE , "<".$_[0] ) or die ( "Failed to open file [".$_[0]."]" ); 
  my $data = eval ( <REC_FILE> );
  close REC_FILE;
  
  return $data;
}

sub reset()
{
 my ($self) = @_ ;
 $self->{RECOVERY} = 0;
 $self->{DATA} = {};
 return $self->commit();
}

sub isInRecovery()
{
 my ($self) = @_ ;
 #print "isInRecovery " .$self->{RECOVERY}  ."\n";
 return $self->{RECOVERY} ; 
}


sub add()
{
 my ( $self , $prop_name , $prop_value , $force_commit) = @_;
 
# print "ADD $prop_name , $prop_value , $force_commit \n";
 SWITCH:
 {
    if(ref ( $prop_value ) =~/SCALAR/) {goto SCALAR;}
    if(ref ( $prop_value ) =~/ARRAY/) {goto ARRAY;}
    if(ref ( $prop_value ) =~/HASH/) {goto HASH;}
   # goto DEFAULT;
    
    SCALAR:
    {
      $self->{DATA}->{$prop_name} = $prop_value;
      last SWITCH; 
    }
    
    ARRAY:
    {
      @{$self->{DATA}->{$prop_name}} = @{$prop_value};
      last SWITCH; 
    }
    
    HASH:
    {
      %{$self->{DATA}->{$prop_name}} = %{$prop_value};
      last SWITCH; 
    }
  }
  
  $self->{COMMIT_BACKLOG} += 1;
    
 ( defined $force_commit || 0 == ( $self->{COMMIT_BACKLOG} % $self->{COMMIT_THRESHOLD} ) )  ? return  $self->commit() : return 1;
}

sub get ()
{
 my ( $self , $prop_name ) = @_;
 
 #print "GET $prop_name " . $self->{DATA}->{$prop_name} ."\n";
 return $self->{DATA}->{$prop_name};
}


sub exists()
{
  my ( $self , $prop_name ) = @_;
  
  #print "EXISTS  $prop_name \n";
  return exists $self->{DATA}->{$prop_name};
}

sub commit()
{
  my ( $self ) = @_;
  
  #print "COMMIT\n";
  
  my $FH = $self->{OUT_FILE}; 
  
  $FH->truncate(0);
  print $FH  Dumper( $self->{DATA} );
  $FH->flush();
 # print "COMMIT\n";
  return 1;
}

sub fini()
{
  my ( $self ) = @_;
  #print "FINI\n";
  my $FH = $self->{OUT_FILE}; 

  $FH->truncate(0);
  $FH->close();
  
  unlink $self->{OUT_FILE_NAME}  or die "Failed to delete recovery file [$!]";
  
  return 1;
 }


1;