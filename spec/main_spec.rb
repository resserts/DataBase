describe 'database' do
  def run_script(commands, reset_data=true)
    raw_output = nil
    if reset_data
      system("rm test.db")
    end
    IO.popen("./main test.db", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'inserts and retrieves a row' do
    result = run_script([
      "insert 1 user1 person1@example.com",
      "print",
      ".exit",
    ])
    expect(result).to match_array([
      "(sqlite)> Inserted.",
      "(sqlite)> (1, user1, person1@example.com)",
      "(sqlite)> ",
    ])
  end

  it 'prints error message when table is full' do
    script = (1..127).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result[-2]).to eq('(sqlite)> Error: Table full.')
  end

  it 'allows inserting strings that are the maximum length' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "print",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "(sqlite)> Inserted.",
      "(sqlite)> (1, #{long_username}, #{long_email})",
      "(sqlite)> ",
    ])
  end

  it 'prints error message when username or email are too long' do
    too_long_username = "a"*32
    too_long_email = "a"*256
    script = ["insert 1 #{too_long_username} #{too_long_email}",
              ".exit",]

    result = run_script(script)
    expect(result).to match_array([
      "(sqlite)> Error: email too long.",
      "(sqlite)> ",
    ])
  end

  it 'prints an error message if id is negative' do
    script = [
      "insert -1 cstack foo@bar.com",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "(sqlite)> ID can't be negative or zero.",
      "(sqlite)> ",
    ])
  end

  it 'checks ordering' do
    script = [
      "insert 2 a a",
      "insert 1 b b",
      "insert 3 c c",
      ".btree",
      ".exit",
    ]

    result = run_script(script)

    expect(result).to match_array([
      "(sqlite)> Inserted.",
      "(sqlite)> Inserted.",
      "(sqlite)> Inserted.",
      "(sqlite)> leaf (size 3).",
      "\t0 : 1",
      "\t1 : 2",
      "\t2 : 3",
      "(sqlite)> ",
    ])
  end

  it 'checks persistence' do
    num_users = 13
    script1 = (1..num_users).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    script1 << ".exit"

    result1 = run_script(script1)

    expected1 = (1..num_users).map do |i|
      "(sqlite)> Inserted."
    end
    expected1 << "(sqlite)> "
    expect(result1).to match_array(expected1)

    script2 = [
      "print",
      ".exit",
    ]
    result2 = run_script(script2, false)

    expected2 = ["(sqlite)> (1, user1, user1@example.com)"]
    (2..num_users).map do |i|
      expected2 << "(#{i}, user#{i}, user#{i}@example.com)"
    end
    expected2 << "(sqlite)> "
    expect(result2).to match_array(expected2)
  end

end
