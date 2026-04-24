export function string_set(): Set<string> {
    return new Set(["a", "b", "d"]);  // Wrong element
}

export function mixed_set(): Set<number | string> {
    return new Set([1, "two", 3]);
}
