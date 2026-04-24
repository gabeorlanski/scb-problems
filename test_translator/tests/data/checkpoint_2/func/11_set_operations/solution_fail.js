export function string_set() {
    return new Set(["a", "b", "d"]);  // Wrong element
}

export function mixed_set() {
    return new Set([1, "two", 3]);
}
