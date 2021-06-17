use strict;
use warnings;
use MapReduce::Framework::Simple;

my $mfs = MapReduce::Framework::Simple->new(
    skip_undef_result => 1,
    warn_discarded_data => 1
   );

my $data_map_reduce = [

    [[{start => 1_000_000_001, end => 1_000_010_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_010_001, end => 1_000_020_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_020_001, end => 1_000_030_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_030_001, end => 1_000_040_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_040_001, end => 1_000_050_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_050_001, end => 1_000_060_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_060_001, end => 1_000_070_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_070_001, end => 1_000_080_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_080_001, end => 1_000_090_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_090_001, end => 1_000_100_000}],'http://172.16.50.16:5000/eval'],


    [[{start => 1_000_100_001, end => 1_000_110_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_110_001, end => 1_000_120_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_120_001, end => 1_000_130_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_130_001, end => 1_000_140_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_140_001, end => 1_000_150_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_150_001, end => 1_000_160_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_160_001, end => 1_000_170_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_170_001, end => 1_000_180_000}],'http://172.16.50.16:5000/eval'],
    [[{start => 1_000_180_001, end => 1_000_190_000}],'http://172.16.50.14:5000/eval'],
    [[{start => 1_000_190_001, end => 1_000_200_000}],'http://172.16.50.16:5000/eval'],

   ];

# mapper code
my $mapper = sub {
    my $input = shift;
    my $start = $input->[0]->{start};
    my $end = $input->[0]->{end};
    my $sum=0;
    for($start .. $end){
        my $flag = 0;
        for( my $k=2; $k <= int(sqrt($_)); $k++){
            if(($_ % $k) == 0){
                $flag = 1;
            }
        }
        if($flag == 0){
            $sum += $_;
        }
    }
    return($sum);
};


# reducer code
my $reducer = sub {
    my $input = shift;
    my $sum = 0;
    for(0 .. $#$input){
        $sum += $input->[$_];
    }
    return($sum);
};

# do
my $result = $mfs->map_reduce(
    $data_map_reduce,
    $mapper,
    $reducer,
    30,
    {remote => 1}
   );

print "$result\n";