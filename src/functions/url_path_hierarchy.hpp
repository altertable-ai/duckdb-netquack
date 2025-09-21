// Copyright 2025 Altertable

#pragma once

#include "duckdb.hpp"
#include <vector>

namespace duckdb
{
    // Function to build URL path hierarchy as LIST(VARCHAR)
    void URLPathHierarchyFunction (DataChunk &args, ExpressionState &state, Vector &result);

    namespace netquack
    {
        // Helper that returns vector of hierarchical path components
        // e.g. "/browse/CONV-6788" -> {"/browse/", "/browse/CONV-6788"}
        std::vector<std::string> BuildURLPathHierarchy (const std::string &input);
    } // namespace netquack
} // namespace duckdb


