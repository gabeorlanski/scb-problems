export function edge_case(): number {
    return 1.001;  // abs(1.001 - 1.0) = 0.001 >= 0.001 (strict <)
}
