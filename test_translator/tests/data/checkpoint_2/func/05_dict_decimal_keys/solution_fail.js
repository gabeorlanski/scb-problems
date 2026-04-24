export function get_decimal_key_dict() {
    return new Map([
        ["1.5", "a"],
        ["2.25", "b"],
        [[3, "x"], "tuple"],
    ]);
}
