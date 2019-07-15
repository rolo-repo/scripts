package Env::Sourced;
use strict;
sub import
{
  my $class = shift;
  my $source_command = '.';
  my $shell_name=$ENV{SHELL}." -c";
  
  my $perl_command = "perl -MData::Dumper -e 'print Dumper(\\\%ENV)';";

  #--> Loop through all of the files passed to the module and:
  #-->   1) Source the file, returning it's output as a hash
  #-->   2) Include the sourced files environment into the current
  #-->      environment.  Any unchanged environment variables should
  #-->      just be passed back to us as-is.
  
  while(my $file = shift)
  {
 
   if(-e $file){
    my $source_line = "$source_command $file 1>&2";
    my %tmp = %{eval('my ' . `$shell_name \"$source_line\n$perl_command\"`)};

    $ENV{$_} = $tmp{$_} for (keys %tmp);
     }
  }
}

1;
