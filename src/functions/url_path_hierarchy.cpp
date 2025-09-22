// Copyright 2025 Altertable

#include "url_path_hierarchy.hpp"
#include "extract_path.hpp"

namespace duckdb
{
    // Scalar function implementation: returns LIST(VARCHAR)
    void URLPathHierarchyFunction (DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Input vector
        auto &input_vector = args.data[0];

        // Result must be a list(vector)
        D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

        // Prepare result as FLAT for writing list entries
        result.SetVectorType(VectorType::FLAT_VECTOR);
        ListVector::SetListSize(result, 0);

        // Accessors
        auto list_entries = FlatVector::GetData<list_entry_t> (result);
        auto &child_entry  = ListVector::GetEntry (result);

        // We'll accumulate all child strings linearly, then set list offsets/lengths
        idx_t total_children = 0;

        for (idx_t row_idx = 0; row_idx < args.size (); row_idx++)
        {
            auto value = input_vector.GetValue (row_idx);

            if (value.IsNull ())
            {
                // NULL input -> NULL list
                FlatVector::Validity (result).SetInvalid (row_idx);
                list_entries[row_idx] = { 0, 0 };
                continue;
            }

            auto input = value.ToString ();
            std::transform (input.begin (), input.end (), input.begin (), ::tolower);

            // Build hierarchy components
            auto components = netquack::BuildURLPathHierarchy (input);

            // Ensure child capacity (simple reserve to required size)
            auto required_children = total_children + components.size ();
            ListVector::Reserve (result, required_children);

            // Write list entry metadata
            list_entries[row_idx].offset = total_children;
            list_entries[row_idx].length = components.size ();

            // Fill child strings
            auto child_data = FlatVector::GetData<string_t> (child_entry);
            for (idx_t k = 0; k < components.size (); k++)
            {
                child_data[total_children + k] = StringVector::AddString (child_entry, components[k]);
            }

            total_children += components.size ();
        }

        // Finalize child count
        ListVector::SetListSize (result, total_children);

        // Add heap reference to keep strings alive
        StringVector::AddHeapReference (ListVector::GetEntry (result), args.data[0]);

        if (args.AllConstant ())
        {
            result.SetVectorType (VectorType::CONSTANT_VECTOR);
        }
    }

    namespace netquack
    {
        std::vector<std::string> BuildURLPathHierarchy (const std::string &input)
        {
            std::vector<std::string> result;
            auto path = ExtractPath (input);

            // Normalize: ensure leading slash if path exists, remove query/fragment handled by regex
            if (path.empty () || path == "/")
            {
                return result; // empty list when only root
            }

            // Split into segments ignoring empty
            // Keep track if original path ended with '/'
            bool ends_with_slash = !path.empty () && path.back () == '/';

            // Remove leading slash for splitting
            std::string trimmed = path;
            if (!trimmed.empty () && trimmed.front () == '/')
            {
                trimmed.erase (trimmed.begin ());
            }

            std::vector<std::string> segments;
            size_t start = 0;
            while (start <= trimmed.size ())
            {
                auto pos = trimmed.find ('/', start);
                if (pos == std::string::npos)
                {
                    auto token = trimmed.substr (start);
                    if (!token.empty ())
                    {
                        segments.push_back (token);
                    }
                    break;
                }
                auto token = trimmed.substr (start, pos - start);
                if (!token.empty ())
                {
                    segments.push_back (token);
                }
                start = pos + 1;
            }

            // Build hierarchy without carrying trailing slash into next iteration
            std::string accum_no_trailing;
            for (idx_t i = 0; i < segments.size (); i++)
            {
                if (!accum_no_trailing.empty ())
                {
                    accum_no_trailing.push_back ('/');
                }
                accum_no_trailing.append (segments[i]);

                std::string out = "/";
                out.append (accum_no_trailing);
                if (i < segments.size () - 1 || ends_with_slash)
                {
                    out.push_back ('/');
                }
                result.push_back (out);
            }

            return result;
        }
    } // namespace netquack
} // namespace duckdb


