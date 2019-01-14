defmodule JoinTest do
  use ExUnit.Case
  doctest Join

  @t1 [
    [a: 1, b: 2],
    [a: 2, b: 4]
  ]
  @t2 [
    [a: 1, c: 3],
    [a: 2, c: 5]
  ]
  @t2_2 [
    [a2: 1, c: 3],
    [a2: 2, c: 5]
  ]
  @t3 [
    [a: 1, b: 2],
    [a: 1, b: 12],
    [a: 2, b: 4]
  ]
  @t4 [
    [a: 1, c: 22],
    [a: 1, c: 23],
    [a: 2, c: 4]
  ]

  @u1 [
    [a: 1, b: 1, c: 3],
    [a: 1, b: 2, c: 4],
    [a: 2, b: 3, c: 5]
  ]
  @u2 [
    [a: 1, b: 1, d: 13],
    [a: 1, b: 2, d: 14],
    [a: 2, b: 3, d: 15]
  ]
  @u2_2 [
    [a2: 1, b: 1, d: 13],
    [a2: 1, b: 2, d: 14],
    [a2: 2, b: 3, d: 15]
  ]

  # ------------------- normal cases ------------------ #
  test "simple inner join" do
    assert Join.inner(@t1, @t2, on: :a) ==
             [
               [a: 1, b: 2, a: 1, c: 3],
               [a: 2, b: 4, a: 2, c: 5]
             ]
  end

  test "simple inner join - delete right key" do
    assert Join.inner(@t1, @t2, on: :a, delete_right_key: true, validation: :one_to_one) ==
             [
               [a: 1, b: 2, c: 3],
               [a: 2, b: 4, c: 5]
             ]
  end

  test "one-to-many inner join" do
    assert Join.inner(@t1, @t4, on: :a, delete_right_key: true, validation: :one_to_many) == [
             [a: 1, b: 2, c: 22],
             [a: 1, b: 2, c: 23],
             [a: 2, b: 4, c: 4]
           ]
  end

  test "many-to-one inner join" do
    assert Join.inner(@t3, @t2, on: :a, delete_right_key: true, validation: :many_to_one) == [
             [a: 1, b: 2, c: 3],
             [a: 1, b: 12, c: 3],
             [a: 2, b: 4, c: 5]
           ]
  end

  test "many-to-many inner join" do
    assert Join.inner(@t3, @t4, on: :a, delete_right_key: true) == [
             [a: 1, b: 2, c: 22],
             [a: 1, b: 2, c: 23],
             [a: 1, b: 12, c: 22],
             [a: 1, b: 12, c: 23],
             [a: 2, b: 4, c: 4]
           ]
  end

  test "simple inner join - different name" do
    assert Join.inner(@t1, @t2_2, on_left: :a, on_right: :a2) ==
             [
               [a: 1, b: 2, a2: 1, c: 3],
               [a: 2, b: 4, a2: 2, c: 5]
             ]
  end

  test "multiple keys" do
    assert Join.inner(@u1, @u2, on: [:a, :b], delete_right_key: true) ==
             [
               [a: 1, b: 1, c: 3, d: 13],
               [a: 1, b: 2, c: 4, d: 14],
               [a: 2, b: 3, c: 5, d: 15]
             ]
  end

  test "multiple keys - different name" do
    assert Join.inner(@u1, @u2_2, on_left: [:a, :b], on_right: [:a2, :b], validation: :one_to_one) ==
             [
               [a: 1, b: 1, c: 3, a2: 1, b: 1, d: 13],
               [a: 1, b: 2, c: 4, a2: 1, b: 2, d: 14],
               [a: 2, b: 3, c: 5, a2: 2, b: 3, d: 15]
             ]
  end

  # -------------------- abormal cases -------------- #
  test "key not found" do
    assert_raise(KeyError, "key :q not found in: [a: 1, b: 2]", fn ->
      Join.inner(@t1, @t2, on: :q)
    end)
  end

  test "number of keys not match" do
    assert_raise(
      ArgumentError,
      "keys have to be the same number of members: [:a, :b], [:a]",
      fn ->
        Join.inner(@t1, @t2, on_left: [:a, :b], on_right: [:a])
      end
    )
  end

  test "empty key" do
    assert_raise(
      ArgumentError,
      "join key is empty.",
      fn -> Join.inner(@t1, @t2, on: []) end
    )
  end

  test "validation failed - many-to-one" do
    assert_raise(RuntimeError, "validation :many_to_one failed. key: [a: 1]", fn ->
      Join.inner(@t1, @t4, on: :a, delete_right_key: true, validation: :many_to_one)
    end)
  end

  test "validation failed - one-to-one" do
    assert_raise(RuntimeError, "validation :one_to_one failed. key: [a: 1]", fn ->
      Join.inner(@t1, @t4, on: :a, delete_right_key: true, validation: :one_to_one)
    end)
  end

  test "validation failure - one-to-many" do
    assert_raise(RuntimeError, "validation :one_to_many failed. key: [a: 1]", fn ->
      Join.inner(@t3, @t2, on: :a, delete_right_key: true, validation: :one_to_many)
    end)
  end
end
